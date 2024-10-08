def handler(data, log):
    data["new_field"] = "new_value"
    log.info(data)
    return data
