class CORSSettings:
    def __init__(
        self,
        allow_origins: list[str],
        allow_methods: list[str],
        allow_headers: list[str],
    ):
        self.allow_origins = allow_origins
        self.allow_methods = allow_methods
        self.allow_headers = allow_headers

    @property
    def serialized_allow_origins(self):
        return b",".join([origin.encode("utf-8") for origin in self.allow_origins])

    @property
    def serialized_allow_methods(self):
        return b",".join([method.encode("utf-8") for method in self.allow_methods])

    @property
    def serialized_allow_headers(self):
        return b",".join([header.encode("utf-8") for header in self.allow_headers])
