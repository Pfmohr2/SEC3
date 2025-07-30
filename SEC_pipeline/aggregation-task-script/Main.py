#!/usr/bin/env python3
"""
Pipeline: Empower Aggregation Pipeline

Implements the Tetra UI query:
    Tween AND "c028556‑2015"

Features:
✔ Full text search: "c028556‑2015*" (prefix match)
✔ Wildcard on filePath: "*Tween*"
✔ Aggregation:
    - Verifies all injections have arrived
    - Groups data by ELN_DocumentID
    - Processes:
        • Standards (RSD %)
        • Check Standards (% Error)
        • Samples (*-T vs *-F pairs)
✔ Output:
    • 1 CSV per ELN_DocumentID
    • File name format:
        SampleSetMethodName-SampleSetID-ELN_DocumentID-SuitabilityStatus.csv
"""

from __future__ import annotations
import json
from typing import Any, Dict, List
import pandas as pd
import os
from io import StringIO, BytesIO
from queries import BASE_QUERY
from fpdf import FPDF
import numpy as np
import sys  # Ensure sys is imported for sys.exit(0)

# -----------------------------------------------------------------------------
# Extract peaks to DataFrame
# -----------------------------------------------------------------------------


def extract_peaks_to_dataframe(peaks: List[Dict[str, Any]], logger) -> pd.DataFrame:
    extracted_data = []
    for peak in peaks:
        try:
            value = peak["amount"]["value"]
            # Skip invalid values per requirements
            if value in [-50000, -2147483648, -32768]:
                continue
            extracted_data.append(
                {
                    "analyte": peak["analyte"],
                    "amount": value,
                }
            )
        except Exception as e:
            logger.log(f"Error processing peak object: {e}")
    return pd.DataFrame(extracted_data)


# -----------------------------------------------------------------------------
# Extract entire document to DataFrame
# -----------------------------------------------------------------------------


def extract_to_dataframe(json_data, logger) -> pd.DataFrame | None:
    count = 0
    final_df = pd.DataFrame()

    try:
        for obj in json_data:
            data = obj.get("data", {})  # Extract 'data' once and reuse

            samples = obj.get("samples", []) or obj.get("data", {}).get("samples", [])
            sample_obj = samples[-1] if samples else {}

            sample_name = sample_obj.get("name", None)
            sample_type = None
            stanadrd_calc_value = None

            for label in sample_obj.get("labels", []):
                if label.get("name") == "Type":
                    sample_type = label.get("value")

            for main_label in obj.get("labels", []):
                if main_label.get("name") == "StdCalcValue":
                    stanadrd_calc_value = main_label.get("value")

            # Extract Sample Set ID
            sample_sets = obj.get("sample_sets", []) or obj.get("data", {}).get(
                "sample_sets", []
            )
            sample_set_id = sample_sets[-1].get("id") if sample_sets else None

            # Extract SampleSetMethodName and ELN_DocumentID
            methods = obj.get("methods", []) or obj.get("data", {}).get("methods", [])

            sample_set_method_name = None
            sample_set_name = None
            injection_id = None
            if methods:
                sample_set = methods[-1].get("sample_set", {})
                method = sample_set.get("method", {})
                sample_set_method_name = method.get("name")
                sample_set_name = sample_set.get("name")
            else:
                logger.log(
                    {
                        "message": "Missing required 'methods' array — cannot extract SampleSetMethodName or ELN_DocumentID",
                        "level": "error",
                    }
                )
                continue
            # Extract ELN_DocumentID from custom_fields
            eln_document_id = None

            if sample_name is None:
                count += 1
                logger.log({"message": "Sample name is None", "level": "warning"})

            file_path = obj.get("filePath", None)

            runs = obj.get("runs", []) or obj.get("data", {}).get("runs", [])

            if runs:
                injection_id = runs[-1].get("injection", {}).get("id", None)
                custom_field_injection = (
                    runs[-1].get("injection", {}).get("custom_fields", [])
                )
                for field in custom_field_injection:
                    if field.get("key") == "ELN_DocumentID":
                        eln_document_id = field.get("value")
            # Use last result
            results = obj.get("results", []) or obj.get("data", {}).get("results", [])
            project_name = obj.get("project", {}).get("name", "") or obj.get(
                "data", {}
            ).get("project", {}).get("name", "")

            if not results:
                file_id = (
                    obj.get("fileId")
                    or obj.get("meta", {}).get("fileId")
                    or obj.get("input_file_pointer", {}).get("meta", {}).get("fileId")
                    or obj.get("file_path")
                    or "unknown"
                )
                logger.log(
                    {
                        "message": f"Results array is empty. fileId: {file_id}, filePath: {obj.get('filePath', 'unknown')}",
                        "level": "error",
                    }
                )
                continue

            # Find result with latest date_processed
            try:
                latest_result = max(results, key=lambda r: r.get("date_processed", ""))
            except Exception as e:
                logger.log(
                    {"message": f"Failed to find latest result: {e}", "level": "error"}
                )
                continue

            peaks = latest_result.get("peaks", [])
            result_id = latest_result.get("id", None)
            if not peaks:
                logger.log({"message": "Peaks array is empty.", "level": "error"})
                continue

            peak_df = extract_peaks_to_dataframe(peaks, logger)
            peak_df["sample_name"] = sample_name
            peak_df["sample_type"] = sample_type
            peak_df["stanadrd_calc_value"] = stanadrd_calc_value
            peak_df["injection_id"] = injection_id
            peak_df["result_id"] = result_id
            peak_df["project_name"] = project_name
            peak_df["result_id"] = result_id
            peak_df["eln_document_id"] = eln_document_id
            peak_df["sample_set_id"] = sample_set_id
            peak_df["sample_set_name"] = sample_set_name
            peak_df["sample_set_method_name"] = sample_set_method_name
            peak_df["file_path"] = file_path

            final_df = pd.concat([final_df, peak_df], ignore_index=True)

        logger.log(
            {
                "message": f"Total records with null sample name: {count}",
                "level": "info",
            }
        )
        return final_df

    except Exception as e:
        logger.log({"message": f"Error processing object: {e}", "level": "error"})
        return None


# -----------------------------------------------------------------------------
# Split DataFrame into categories
# -----------------------------------------------------------------------------


def split_sample_categories(df: pd.DataFrame, logger) -> Dict[str, pd.DataFrame]:
    # Split into categories
    df_standards = df[df["sample_type"] == "Standard"]
    if df_standards["amount"].isnull().any():
        logger.log(
            {
                "message": "Peak value is empty in 'amount' column for Standards. Exiting.",
                "level": "error",
            }
        )
        return {}

    logger.log({"message": f"Standards count: {len(df_standards)}", "level": "info"})

    df_check_standards = df[df["sample_name"].str.lower().str.startswith("check")]
    if df_check_standards["amount"].isnull().any():
        logger.log(
            {
                "message": "Peak value is empty in 'amount' column for Check Standards. Exiting.",
                "level": "error",
            }
        )
        return {}

    logger.log(
        {
            "message": f"Check Standards count: {len(df_check_standards)}",
            "level": "info",
        }
    )

    df_samples = df[
        df["sample_name"].str.endswith("-T", na=False)
        | df["sample_name"].str.endswith("-F", na=False)
    ]
    if df_samples["amount"].isnull().any():
        logger.log(
            {
                "message": "Peak value is empty in 'amount' column for Samples T and F. Exiting.",
                "level": "error",
            }
        )
        return {}

    logger.log({"message": f"Samples count: {len(df_samples)}", "level": "info"})

    return {
        "standards": df_standards,
        "check_standards": df_check_standards,
        "samples": df_samples,
    }


# -----------------------------------------------------------------------------
# Fetch data from API
# -----------------------------------------------------------------------------


def fetch_data(context, query: dict, logger) -> tuple[pd.DataFrame, int]:
    resp = context.search_eql(payload=query)
    hits = resp["hits"]["hits"]  # ← all injections that matched EQL
    total_injections = len(hits)  # ← what you want to compare to NumOfInjs

    # keep only the ones that have usable results, as you already do
    df_valid = extract_to_dataframe([h["_source"] for h in hits], logger)
    return df_valid, total_injections


# -----------------------------------------------------------------------------
# Preprocess sample names
# -----------------------------------------------------------------------------


def preprocess_sample_name(df: pd.DataFrame, logger) -> pd.DataFrame:
    df["base_sample_name"] = df["sample_name"].str.rstrip("-TF").str.strip()
    return df


def save_to_pdf(
    merged_df,
    df_check_standards,
    df_standard_rsd,
    output_file_name,
    context,
    logger,
    message=None,
):
    """
    Save merged_df and df_check_standards into a PDF file as tables with headers and values.
    Adjust column width dynamically for single-value columns like 'amount'.
    Reduce table header font size for compact layout.
    """
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Arial", size=12)

    def add_table_to_pdf(pdf, df, heading, heading_font_size=10, cell_font_size=7):
        # Add heading
        pdf.set_font("Arial", style="B", size=heading_font_size)
        pdf.cell(0, 10, heading, ln=True, align="C")
        pdf.set_font("Arial", size=cell_font_size)

        # Calculate column widths dynamically based on content and header length
        max_lengths = df.astype(str).applymap(len).max(axis=0)
        header_lengths = [len(str(header)) for header in df.columns]
        combined_lengths = [
            max(content, header) for content, header in zip(max_lengths, header_lengths)
        ]
        page_width = pdf.w - 2 * pdf.l_margin
        total_length = sum(combined_lengths)
        col_widths = [
            (page_width * (length / total_length)) for length in combined_lengths
        ]

        # Add table headers
        pdf.set_font("Arial", style="B", size=8)
        for col, width in zip(df.columns, col_widths):
            pdf.cell(width, 8, str(col), border=1, align="C")
        pdf.ln()

        # Add table rows
        pdf.set_font("Arial", size=cell_font_size)
        for _, row in df.iterrows():
            for value, width in zip(row, col_widths):
                pdf.cell(width, 8, str(value), border=1, align="C")
            pdf.ln()

        # Add space after the table
        pdf.ln(10)

    if message:
        # Add message to the PDF
        pdf.set_font("Arial", size=12)
        pdf.multi_cell(0, 10, message, align="C")
        pdf.ln(10)

    # Add merged_df table
    add_table_to_pdf(pdf, merged_df, "Hydrolysis Result")

    # Add df_check_standards table
    add_table_to_pdf(pdf, df_check_standards, "% Recovery Result")

    # Add Standard RSD Result table
    add_table_to_pdf(pdf, df_standard_rsd, "Standard RSD Result")

    # Save the PDF content to a string
    pdf_byte_content = pdf.output(dest="S").encode("latin1")

    # Write the PDF content using context
    context.write_file(
        content=pdf_byte_content,
        file_name=f"{output_file_name}.pdf",
        file_category="PROCESSED",
    )

    logger.log(
        {"message": f"PDF saved as '{output_file_name}' successfully.", "level": "info"}
    )


def save_data_and_message(
    df: pd.DataFrame, message: str, logger, output_file_name: str
) -> None:
    """
    Save DataFrame data and a message to a file.

    Args:
        df (pd.DataFrame): The DataFrame containing data to save.
        message (str): The message to include in the file.
        output_file_name (str): The name of the output file.
    """
    with open(f"{output_file_name}.pdf", "w") as f:
        # Write the message
        f.write(f"Message:\n{message}\n\n")

        # Write the DataFrame data
        f.write("Data:\n")
        df.to_csv(f, index=False)

    logger.log(f"Data and message saved to '{output_file_name}' successfully.")


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------


def check_num_injections(actual_injs: int, expected_num_injs: int, logger) -> bool:
    logger.log(
        {
            "message": f"Expected NumOfInjs: {expected_num_injs}, "
            f"Received: {actual_injs}",
            "level": "info",
        }
    )
    return actual_injs >= expected_num_injs


def calculate_standards_rsd(df_standards: pd.DataFrame, logger) -> str:
    """
    Calculate Standards RSD% and return the suitability status, with detailed logging.
    """
    if not df_standards.empty and len(df_standards) == 6:
        if df_standards["stanadrd_calc_value"].isnull().any():
            logger.log(
                {
                    "message": "Some stanadrd_calc_value values are missing. Cannot compute RSD.",
                    "level": "error",
                }
            )
            return "FAIL", pd.DataFrame()
        df_standards["stanadrd_calc_value"] = df_standards[
            "stanadrd_calc_value"
        ].astype(float)
        std_dev = np.std(df_standards["stanadrd_calc_value"].to_list(), ddof=1)
        mean = np.mean(df_standards["stanadrd_calc_value"].to_list())
        rsd_percent = float("inf") if mean == 0 else abs(std_dev / mean) * 100

        logger.log(
            {
                "message": f"Standard deviation of differences: {std_dev}",
                "level": "info",
            }
        )
        logger.log(
            {
                "message": f"Mean of differences: {mean}",
                "level": "info",
            }
        )
        logger.log(
            {
                "message": f"Standards RSD% = {rsd_percent:.2f}%",
                "level": "info",
            }
        )

        status = "PASS" if rsd_percent <= 3.0 else "FAIL"
        df_standard_rsd = pd.DataFrame({"Standard RSD": [rsd_percent]})
        return status, df_standard_rsd
    else:
        logger.log(
            {
                "message": "No Standards found or less than 6 — cannot compute RSD.",
                "level": "warning",
            }
        )
        return "FAIL", pd.DataFrame()


def process_check_standards(df_check_standards: pd.DataFrame, logger) -> str:
    """
    Process Check Standards and return the overall status, with detailed logging.
    """
    THEORETICAL_VALUE = float(os.getenv("theoretical_value", "0.079"))
    logger.log(
        {
            "message": f"Processing Check Standards with THEORETICAL_VALUE = {THEORETICAL_VALUE}",
            "level": "info",
        }
    )

    if not df_check_standards.empty:
        logger.log(
            {
                "message": f"Raw Check Standards amounts: {df_check_standards['amount'].tolist()}",
                "level": "info",
            }
        )

        df_check_standards["percent_error"] = 100 - (
            (df_check_standards["amount"] / THEORETICAL_VALUE) * 100
        )

        df_check_standards["%Recovery"] = (
            (df_check_standards["amount"] / THEORETICAL_VALUE) * 100
        ).round(6)

        logger.log(
            {
                "message": f"%Recovery values: {df_check_standards['%Recovery'].tolist()}",
                "level": "info",
            }
        )
        logger.log(
            {
                "message": f"Percent errors: {df_check_standards['percent_error'].tolist()}",
                "level": "info",
            }
        )

        df_check_standards["status"] = df_check_standards["percent_error"].apply(
            lambda x: "PASS" if abs(x) <= 15 else "FAIL"
        )

        logger.log(
            {
                "message": f"Statuses: {df_check_standards['status'].tolist()}",
                "level": "info",
            }
        )

        status = "FAIL" if (df_check_standards["status"] == "FAIL").any() else "PASS"

        logger.log(
            {
                "message": f"Overall Check Standards status: {status}",
                "level": "info",
            }
        )

        df_check_standards = df_check_standards[
            [
                "sample_set_id",
                "sample_set_name",
                "sample_name",
                "injection_id",
                "%Recovery",
                "status",
            ]
        ]

        return status, df_check_standards
    else:
        logger.log(
            {
                "message": "No Check Standards found — cannot compute Error.",
                "level": "warning",
            }
        )
        return "FAIL", pd.DataFrame()


def validate_eln_document_id(df_test: pd.DataFrame, logger) -> bool:
    df_samples = preprocess_sample_name(df_test, logger)
    df_left = df_samples[df_samples["sample_name"].str.endswith("-T")]
    df_right = df_samples[df_samples["sample_name"].str.endswith("-F")]
    merged_df = pd.merge(
        df_left,
        df_right,
        on=[
            "analyte",
            "base_sample_name",
            "sample_set_id",
            "sample_set_method_name",
        ],
        how="inner",
        suffixes=("_T", "_F"),
    )
    mismatched_ids = merged_df[
        merged_df["eln_document_id_T"] != merged_df["eln_document_id_F"]
    ]
    if not mismatched_ids.empty:
        logger.log(
            {
                "message": "Mismatch found in eln_document_id_T and eln_document_id_F.",
                "level": "error",
            }
        )
        return True
    return False


def process_T_F_samples(df_samples, logger):
    """
    Process T and F samples by merging and calculating the final result.
    If any critical column contains None after the merge, return an empty DataFrame.

    Args:
        df_samples (pd.DataFrame): The DataFrame containing T and F samples.
        logger: Logger object for logging messages.

    Returns:
        pd.DataFrame: The processed DataFrame or an empty DataFrame if invalid data is found.
    """
    df_samples = preprocess_sample_name(df_samples, logger)
    df_left = df_samples[df_samples["sample_name"].str.endswith("-T")]
    df_right = df_samples[df_samples["sample_name"].str.endswith("-F")]

    if not df_left.empty and not df_right.empty:
        merged_df = pd.merge(
            df_left,
            df_right,
            on=[
                "analyte",
                "base_sample_name",
                "sample_set_id",
                "eln_document_id",
                "sample_set_name",
                "sample_set_method_name",
            ],
            how="outer",
            suffixes=("_T", "_F"),
        )

        # Check for None in critical columns
        critical_columns = ["amount_T", "amount_F"]
        if merged_df[critical_columns].isnull().any().any():
            logger.log(
                {
                    "message": "either T sample exist and corresponding F is not available or vice versa.",
                    "level": "error",
                }
            )
            return pd.DataFrame()

        # Calculate the final result
        merged_df["ps_80_final_result"] = (
            merged_df["amount_T"] - merged_df["amount_F"]
        ).round(6)

        return merged_df

    else:
        logger.log(
            {
                "message": "Either df_left (T) or df_right (F) is empty. Cannot process samples.",
                "level": "warning",
            }
        )
        return pd.DataFrame()


def process_samples(df_samples: pd.DataFrame, logger) -> None:
    """
    Process T and F samples and save the results to a file.
    """
    if df_samples.empty:
        logger.log(
            {"message": "No Samples T and F found. Exiting.", "level": "warning"}
        )
        return [pd.DataFrame()], False

    missmatch_eln_ids = validate_eln_document_id(df_samples, logger)
    output_df = []
    if missmatch_eln_ids:
        output_df.append(process_T_F_samples(df_samples, logger))
    else:
        for group, df_group in df_samples.groupby(["eln_document_id"]):
            output_df.append(process_T_F_samples(df_group, logger))

    return output_df, missmatch_eln_ids


def flatten_columns_to_single_column(df: pd.DataFrame, logger) -> pd.DataFrame:
    """
    Transform DataFrame values into a single column by transposing row by row without including headers.

    Args:
        df (pd.DataFrame): The DataFrame to transform.

    Returns:
        pd.DataFrame: The transformed DataFrame with values in a single column.
    """
    flattened_data = []
    for row in df.itertuples(index=False):
        flattened_data.extend(row)  # Add row values sequentially

    # Create a new DataFrame with a single column
    flattened_df = pd.DataFrame(flattened_data, columns=["Unique"])
    return flattened_df


def rename_columns_and_transpose(df: pd.DataFrame, logger) -> pd.DataFrame:
    """
    Rename columns in the DataFrame based on the provided mapping.

    Args:
        df (pd.DataFrame): The DataFrame to rename columns.

    Returns:
        pd.DataFrame: The DataFrame with renamed columns.
    """
    column_mapping = {
        "amount_F": "FOA value",
        "result_id_F": "FOA Result ID",
        "project_name_F": "Project ID (F)",
        "amount_T": "TOA value",
        "result_id_T": "TOA Result ID",
        "project_name_T": "Project ID (T)",
        "ps_80_final_result": "Polysorbate 80",
    }
    df = df.rename(columns=column_mapping)
    df.sort_values(by=["base_sample_name"], ascending=True, inplace=True)
    # Only round FOA and TOA values
    for col in ["FOA value", "TOA value"]:
        if col in df.columns:
            df[col] = df[col].astype(float).round(6)
    df = df[
        [
            "FOA value",
            "FOA Result ID",
            "Project ID (F)",
            "TOA value",
            "Polysorbate 80",
            "TOA Result ID",
            "Project ID (T)",
        ]
    ]
    return flatten_columns_to_single_column(df, logger)


def save_csv(df: pd.DataFrame, output_file_name: str, context, logger) -> None:
    buffer = StringIO()
    df.to_csv(buffer, index=False)
    byte_content = buffer.getvalue().encode("utf-8")

    context.write_file(
        content=byte_content,
        file_name=output_file_name,
        file_category="PROCESSED",
    )
    logger.log(
        {"message": f"CSV saved as '{output_file_name}' successfully.", "level": "info"}
    )


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------


def main(input: dict, context: object) -> None:
    logger = context.get_logger()
    logger.log({"message": f"Received input: {json.dumps(input)}", "level": "debug"})

    if "input_file_pointer" not in input:
        logger.log(
            {"message": "Missing 'input_file_pointer' in input", "level": "error"}
        )
        return

    file_pointer = input["input_file_pointer"]

    file_id = (
        file_pointer["fileId"]
        if isinstance(file_pointer, dict) and "fileId" in file_pointer
        else file_pointer
    )

    labels = context.get_labels(file_pointer)

    label_dict = {label["name"]: label["value"] for label in labels}
    logger.log({"message": f"Labels fetched: {str(label_dict)}", "level": "debug"})
    try:
        ids_file_data_raw = context.read_file({"fileId": file_id}, form="body")[
            "body"
        ].decode("utf-8")

        ids_file_data = json.loads(ids_file_data_raw)

    except Exception as e:
        logger.log(
            {
                "message": f"Exception during read_file of filepointer or json.loads: {str(e)}",
                "level": "error",
            }
        )
        return

    df_ids_data = extract_to_dataframe([ids_file_data], logger)
    if df_ids_data is None or df_ids_data.empty:
        logger.log(
            {
                "message": "Input Filepointer has empty data.",
                "level": "error",
            }
        )
        return

    sample_set_id = df_ids_data.iloc[0]["sample_set_id"]

    query = json.loads(json.dumps(BASE_QUERY).replace("{sample_set_id}", sample_set_id))

    logger.log({"message": f"Query used: {json.dumps(query)}", "level": "debug"})
    df_fetched, total_injections = fetch_data(context, query, logger)
    df_fetched = df_fetched[df_fetched["analyte"] == "Oleic Acid"]
    if df_fetched is None or df_fetched.empty:
        logger.log(
            {
                "message": "Fetched DataFrame is None or empty. Exiting.",
                "level": "error",
            }
        )
        return

    expected_num_injs = label_dict.get("NumOfInjs", None)

    if expected_num_injs is None:
        logger.log(
            {"message": "NumOfInjs label not found in IDS. Exiting.", "level": "error"}
        )
        return

    if expected_num_injs is None:
        logger.log(
            {"message": "NumOfInjs label not found in IDS. Exiting.", "level": "error"}
        )
        return

    # Check number of injections
    if not check_num_injections(total_injections, int(expected_num_injs), logger):
        logger.log(
            {
                "message": (
                    "Not all relevant files have arrived in TDP yet "
                    f"(expected {expected_num_injs}, got {total_injections})."
                ),
                "level": "info",
            }
        )
        return

    df_categories = split_sample_categories(df_fetched, logger)
    if not df_categories:
        logger.log(
            {
                "message": "No valid categories found in the DataFrame. Exiting.",
                "level": "error",
            }
        )
        return

    df_standards = df_categories["standards"]
    df_check_standards = df_categories["check_standards"]
    df_samples = df_categories["samples"]

    # Process Standards
    standards_status, df_standard_rsd = calculate_standards_rsd(df_standards, logger)
    if df_standard_rsd.empty:
        logger.log({"message": "No valid Standards found. Exiting.", "level": "error"})
        return
    logger.log(
        {
            "message": f"Standards RSD files: {str(df_standards['file_path'].unique())}",
            "level": "info",
        }
    )
    df_standard_rsd["status"] = standards_status
    df_standard_rsd["Standard RSD"] = df_standard_rsd["Standard RSD"].round(2)
    # Process Check Standards
    check_standards_status, df_check_standard = process_check_standards(
        df_check_standards, logger
    )
    if df_check_standard.empty:
        logger.log(
            {"message": "No valid Check Standards found. Exiting.", "level": "error"}
        )
        return

    logger.log(
        {
            "message": f" Check Standards files: {str(df_check_standards['file_path'].unique())}",
            "level": "info",
        }
    )
    # Determine overall suitability status
    suitability_status = [standards_status] + [check_standards_status]
    overall_status = "FAIL" if "FAIL" in suitability_status else "PASS"

    sample_set_name = df_fetched["sample_set_name"].iloc[0]

    # Process Samples
    sample_columns = [
        "sample_set_name",
        "result_id_F",
        "result_id_T",
        "base_sample_name",
        "ps_80_final_result",
    ]

    logger.log(
        {
            "message": f" T and F file paths: {str(df_samples['file_path'].unique())}",
            "level": "info",
        }
    )
    output_df, missmatch_eln_ids = process_samples(df_samples, logger)
    if missmatch_eln_ids:
        output_df_final = output_df[0][sample_columns]
        output_df_final["ps_80_final_result"] = output_df_final[
            "ps_80_final_result"
        ].round(6)
        if output_df_final.empty:
            logger.log(
                {
                    "message": "No valid T and F samples found after processing. Exiting.",
                    "level": "error",
                }
            )
            return
        unique_df = rename_columns_and_transpose(output_df[0], logger)
        file_name = f"{sample_set_name}-{str(sample_set_id)}-{overall_status}"
        message = "Mismatch found in eln_document_id_T and eln_document_id_F"
        save_to_pdf(
            output_df_final,
            df_check_standard,
            df_standard_rsd,
            file_name,
            context,
            logger,
            message=message,
        )
        save_csv(unique_df, f"{file_name}.csv", context=context, logger=logger)

    else:
        for df_group in output_df:
            if df_group.empty:
                logger.log(
                    {
                        "message": "No valid T and F samples found after processing. Exiting.",
                        "level": "error",
                    }
                )
                return
            unique_df = rename_columns_and_transpose(df_group, logger)
            file_name = f"{df_group['sample_set_name'].iloc[0]}-{str(df_group['sample_set_id'].iloc[0])}-{overall_status}-{df_group['eln_document_id'].iloc[0]}"
            df_group = df_group[sample_columns]
            df_group["ps_80_final_result"] = df_group["ps_80_final_result"].round(6)
            save_to_pdf(
                df_group, df_check_standard, df_standard_rsd, file_name, context, logger
            )
            save_csv(unique_df, f"{file_name}.csv", context=context, logger=logger)