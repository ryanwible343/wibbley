from wibbley.examples.full_featureset.services.handle_basic_command import listener
from wibbley.messagebus import Messagebus

messagebus = Messagebus(listeners=[listener])
