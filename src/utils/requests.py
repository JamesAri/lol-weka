def construct_query_params(**params):
    query_params = ""
    for param_key in params:
        param_value = params[param_key]
        if param_value:
            query_params += f"&{param_key}={param_value}"
    if query_params:
        # replace first '&' with '?'
        query_params = '?' + query_params[1:]
    return query_params
