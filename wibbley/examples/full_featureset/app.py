import logging

from wibbley.app import App, CORSSettings
from wibbley.examples.full_featureset.messagebus import messagebus
from wibbley.examples.full_featureset.router import router

logging.basicConfig(level=2, format="%(asctime)-15s %(levelname)-8s %(message)s")


cors_settings = CORSSettings(
    allow_origins=["http://localhost:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)


app = App()
app.add_router(router)
app.enable_cors(cors_settings)
app.add_messagebus(messagebus)
