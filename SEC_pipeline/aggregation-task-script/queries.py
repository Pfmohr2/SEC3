BASE_QUERY={

    "size": 1000,
    "query": {
        "bool": {
            "must": [
                {
                    "nested": {
                        "path": "data.sample_sets",
                        "query": {
                            "bool": {
                                "must": [
                                    {
                                        "match": {
                                            "data.sample_sets.id": "{sample_set_id}"
                                        }
                                    }
                                    
                                ]
                            }
                        }
                    }
                },
                {
                    "nested": {
                        "path": "labels",
                        "query": {
                            "bool": {
                                "must": [
                                    {
                                        "match": {
                                            "labels.name": "reference"
                                        }
                                    },
                                    {
                                        "wildcard": {
                                            "labels.value": "c028556-2015*"
                                        }
                                    }
                                ]
                            }
                        }
                    }
                }
                
            ]
        }
    }
}
 