import os
from jinja2 import Template
from utils.env_loader import load_env_file


def render_file(input_path, output_path):
    with open(input_path) as f:
        template = Template(f.read())
    rendered = template.render(
        PROTOCOL_VERSION=os.environ["PROTOCOL_VERSION"],
        SCRIPT_VERSION=os.environ["SCRIPT_VERSION"],
        NAMESPACE=os.environ["NAMESPACE"],
        ORG_SLUG=os.environ["ORG_SLUG"],
    )
    with open(output_path, "w") as f:
        f.write(rendered)


def main():
    load_env_file(levels_up=2)
    render_file(
        "SEC_pipeline/aggregation-protocol/protocol.template.yml",
        "SEC_pipeline/aggregation-protocol/protocol.yml",
    )

    render_file(
        "SEC_pipeline/aggregation-task-script/manifest.template.json",
        "SEC_pipeline/aggregation-task-script/manifest.json",
    )


if __name__ == "__main__":
    main()