import json




def raise_custom_error(status, reason, input_path="Unknown", custom_wrapper="GLUE_CUSTOM_PAYLOAD"):

    """
    **Packages a custom JSON error payload
    (mostly for Step Functions to catch).
    """

    payload = {
        "status": status,
        "failed_file_path": input_path,
        "reason": reason
    }

    json_string = json.dumps(payload)

    print(f"### -{str(custom_wrapper)}|{str(json_string)}|{str(custom_wrapper)}")
    raise Exception(f"{str(custom_wrapper)}|{json_string}|{str(custom_wrapper)}")