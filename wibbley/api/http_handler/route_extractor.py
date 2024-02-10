import re


class RouteInfo:
    def __init__(self, route_func, path_parameters, available_methods):
        self.route_func = route_func
        self.available_methods = available_methods
        self.path_parameters = path_parameters


class RouteExtractor:
    def _match_and_extract(self, pattern, path):
        # Define the regex pattern to capture any value surrounded by {}
        wildcard_pattern = re.sub(r"\{([^{}]+)\}", r"([^/]+)", pattern)

        # Compile the regex pattern
        regex = re.compile(wildcard_pattern)

        # Check if the path matches the regex pattern
        match = regex.fullmatch(path)

        if match:
            # Extract the values from the dynamic parts
            dynamic_path_keys = re.findall(r"\{([^{}]+)\}", pattern)
            dynamic_path_values = match.groups()
            path_parameters = {
                name: value
                for name, value in zip(dynamic_path_keys, dynamic_path_values)
            }
            return True, path_parameters
        return False, []

    def extract(self, routes, request_path, request_method):
        paths = routes.keys()
        for route_path in paths:
            match, path_parameters = self._match_and_extract(route_path, request_path)
            if match:
                available_methods = routes[route_path].keys()
                route_func = routes[route_path].get(request_method, None)
                return RouteInfo(
                    route_func=route_func,
                    path_parameters=path_parameters,
                    available_methods=available_methods,
                )
        return RouteInfo(route_func=None, path_parameters=[], available_methods=[])
