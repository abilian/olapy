from __future__ import absolute_import

try:
    # if pyspark install, use it instead of pandas

    import pyspark
    from .spark.xmla_discover_request_handler import (
        SparkXmlaDiscoverReqHandler as XmlaDiscoverReqHandler,
    )
    from .spark.xmla_execute_request_handler import (
        SparkXmlaExecuteReqHandler as XmlaExecuteReqHandler,
    )

except ImportError:

    from .xmla_discover_request_handler import XmlaDiscoverReqHandler  # type: ignore
    from .xmla_execute_request_handler import XmlaExecuteReqHandler  # type: ignore
