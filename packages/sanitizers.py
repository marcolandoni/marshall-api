# ---------------------------------------------------------------------------
#  Input sanitization helpers
# ---------------------------------------------------------------------------

import re
SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_ ]+$")
# Values that are used verbatim in SQL WHERE clauses in the models
ALLOWED_MWL_VALUES = {
    "inbox",
    "pending observation",
    "review for followup",
    "following",
    "followup complete",
    "archive",
    "allObsQueue",
    "unarchive"
}

ALLOWED_AWL_VALUES = {
    "queued for atel",
    "released",
    "not to be released",
}


def _sanitize_identifier(value):
    """
    Allow only simple SQL identifiers (letters, numbers, underscore).
    Returns None if invalid.
    """
    if value is None:
        return None
    s = str(value)
    if SAFE_IDENTIFIER_RE.match(s):
        return s
    return None


def _sanitize_string(value, max_length=255):
    """
    Basic string sanitization for values that will be interpolated
    into SQL string literals in the legacy models.

    - Casts to string
    - Trims to max_length
    - Escapes quotes and backslashes to reduce injection risk
    """
    if value is None:
        return ""
    s = str(value)[:max_length]
    # Escape backslashes first, then quotes
    s = s.replace("\\", "\\\\")
    s = s.replace('"', '\\"').replace("'", "\\'")
    return s


def _sanitize_get_transients_request(raw):
    """
    Sanitize the payload used by models_transients_get.
    We keep only known / safe keys and coerce them to safe values.
    """
    if not isinstance(raw, dict):
        return {}

    cleaned = {}
    # Free-text search is already heavily cleaned in the model,
    # but we still remove obviously dangerous characters here.
    number_of_detected_params = 0
    q = raw.get("q")
    if q is not None:
        cleaned["q"] = re.sub(r"[^A-Za-z0-9]", "", str(q))[:100]
        number_of_detected_params = number_of_detected_params + 1

    mwl = raw.get("mwl")
    if mwl is not None:
        mwl_str = str(mwl)
        if mwl_str in ALLOWED_MWL_VALUES:
            cleaned["mwl"] = mwl_str
            number_of_detected_params = number_of_detected_params + 1

    awl = raw.get("awl")
    if awl is not None:
        awl_str = str(awl)
        if awl_str in ALLOWED_AWL_VALUES:
            cleaned["awl"] = awl_str
            number_of_detected_params = number_of_detected_params + 1

    cf = raw.get("cf")
    if cf is not None:
        # classifiedFlag is stored as "1"/"0" in SQL
        cleaned["cf"] = "1" if str(cf) in ("1", "true", "True", "yes") else "0"
        number_of_detected_params = number_of_detected_params + 1

    if "snoozed" in raw:
        snoozed = raw.get("snoozed")
        cleaned["snoozed"] = bool(snoozed in (True, "True", "1", 1))
        number_of_detected_params = number_of_detected_params + 1

    if number_of_detected_params > 1:
        return None

    # Column / sort identifiers
    for key in ("filterBy1", "filterBy2", "sortBy"):
        val = raw.get(key)
        ident = _sanitize_identifier(val)
        if ident:
            cleaned[key] = ident

    # Comparison operators – restrict to a known safe set
    op_mapping = {
        "eq": "=",
        "lt": "<",
        "gt": ">",
        "neq": "!=",
        "=": "=",
        "<": "<",
        ">": ">",
        "!=": "!=",
    }
    for key in ("filterOp1", "filterOp2"):
        op = raw.get(key)
        if op is None:
            continue
        op_norm = str(op).lower()
        if op_norm in op_mapping:
            cleaned[key] = op_mapping[op_norm]

    # Filter values – allow numbers; otherwise strip obviously dangerous chars
    for key in ("filterValue1", "filterValue2"):
        if key not in raw:
            continue
        val = raw.get(key)
        if isinstance(val, (int, float)):
            cleaned[key] = val
        else:
            cleaned[key] = re.sub(r'[\"\'`;]', "", str(val))[:255]


    # TCS-related parameters
    if "tcsRank" in raw:
        try:
            cleaned["tcsRank"] = int(raw.get("tcsRank"))
        except (TypeError, ValueError):
            pass

    if "tcsCatalogueId" in raw:
        try:
            cleaned["tcsCatalogueId"] = int(raw.get("tcsCatalogueId"))
        except (TypeError, ValueError):
            pass

    # Pagination and limits
    if "limit" in raw:
        try:
            limit = int(raw.get("limit"))
            if limit < 1:
                limit = 1
            if limit > 20000:
                limit = 20000
            cleaned["limit"] = limit
        except (TypeError, ValueError):
            pass

    if "pageStart" in raw:
        try:
            page_start = int(raw.get("pageStart"))
            if page_start < 0:
                page_start = 0
            cleaned["pageStart"] = page_start
        except (TypeError, ValueError):
            pass

    # Format and sort direction (low risk, but normalise anyway)
    format_ = raw.get("format")
    if format_ in ("html_table", "html_tickets", "json"):
        cleaned["format"] = format_

    if "sortDesc" in raw:
        sort_desc = raw.get("sortDesc")
        cleaned["sortDesc"] = bool(sort_desc in (True, "True", "1", 1))

    return cleaned


def _sanitize_patch_or_classify_request(raw):
    """
    Sanitize the payload used by models_transients_element_put
    (both /patchTransient and /classifyTransient).
    """
    if not isinstance(raw, dict):
        raise ValueError("Request body must be a JSON object")

    cleaned = {}

    # Required identifier
    element_id = raw.get("elementId")
    try:
        cleaned["elementId"] = int(element_id)
    except (TypeError, ValueError):
        raise ValueError("Invalid elementId")

    # Workflow targets – the model will further validate allowed values
    mwl = raw.get("mwl")
    if mwl is not None:
        mwl_str = str(mwl)
        if mwl_str in ALLOWED_MWL_VALUES:
            cleaned["mwl"] = mwl_str

    awl = raw.get("awl")
    if awl is not None:
        awl_str = str(awl)
        if awl_str in ALLOWED_AWL_VALUES:
            cleaned["awl"] = awl_str

    if "snoozed" in raw:
        cleaned["snoozed"] = bool(raw.get("snoozed") in (True, "True", "1", 1))

    # PI details
    if "piName" in raw:
        cleaned["piName"] = _sanitize_string(raw.get("piName"), max_length=255)
    if "piEmail" in raw:
        cleaned["piEmail"] = _sanitize_string(raw.get("piEmail"), max_length=255)

    # Observation / classification priorities
    if "observationPriority" in raw:
        try:
            prio = int(raw.get("observationPriority"))
            # Model expects a small integer; clamp to a reasonable range
            if prio < 0:
                prio = 0
            if prio > 4:
                prio = 4
            cleaned["observationPriority"] = prio
        except (TypeError, ValueError):
            pass

    # Classification fields (for /classifyTransient)
    for key in ("clsType", "clsSource", "clsSnClassification", "clsClassificationWRTMax"):
        if key in raw:
            cleaned[key] = _sanitize_string(raw.get(key), max_length=64)

    if "clsObsdate" in raw:
        v = str(raw.get("clsObsdate"))
        # Strict YYYY-MM-DD format
        if re.match(r"^\d{4}-\d{2}-\d{2}$", v):
            cleaned["clsObsdate"] = v

    if "clsRedshift" in raw:
        v = str(raw.get("clsRedshift")).strip()
        if v:
            try:
                float(v)
                cleaned["clsRedshift"] = v
            except ValueError:
                pass

    if "clsClassificationPhase" in raw:
        v = str(raw.get("clsClassificationPhase")).strip()
        if v:
            try:
                float(v)
                cleaned["clsClassificationPhase"] = v
            except ValueError:
                pass

    if "clsPeculiar" in raw:
        cleaned["clsPeculiar"] = bool(raw.get("clsPeculiar") in (True, "True", "1", 1))

    if "clsSendTo" in raw:
        cleaned["clsSendTo"] = _sanitize_string(raw.get("clsSendTo"), max_length=32)

    # JWT identity, used in several models for logging / auditing
    if "authenticated_userid" in raw:
        cleaned["authenticated_userid"] = _sanitize_string(
            raw.get("authenticated_userid"), max_length=255
        )

    return cleaned


def _sanitize_comment_request(raw):
    """
    Sanitize the payload used by models_transients_comments_put.
    """
    if not isinstance(raw, dict):
        raise ValueError("Request body must be a JSON object")

    cleaned = {}

    element_id = raw.get("elementId")
    try:
        cleaned["elementId"] = int(element_id)
    except (TypeError, ValueError):
        raise ValueError("Invalid elementId")

    if "comment" not in raw:
        raise ValueError("Missing comment")

    cleaned["comment"] = _sanitize_string(raw.get("comment"), max_length=2000)

    if "authenticated_userid" in raw:
        cleaned["authenticated_userid"] = _sanitize_string(
            raw.get("authenticated_userid"), max_length=255
        )

    return cleaned


def _sanitize_count_transients_request(raw):
    """
    Sanitize the payload used by models_transients_count.
    """
    if not isinstance(raw, dict):
        return {}

    cleaned = {}

    mwf_flag = raw.get("mwfFlag")
    awf_flag = raw.get("awfFlag")

    # Check consistency between mwfFlag and awfFlag
    if mwf_flag is not None and awf_flag is not None:

        raise ValueError("Both mwfFlag and awfFlag cannot be provided simultaneously.")

    if mwf_flag is not None:
        # meta_workflow_lists_counts.listName values – use identifier sanitizer
        ident = _sanitize_identifier(mwf_flag)
        if ident and ALLOWED_MWL_VALUES:
            cleaned["mwfFlag"] = ident

    if awf_flag is not None:
        ident = _sanitize_identifier(awf_flag)
        if ident and ident in ALLOWED_AWL_VALUES:
            cleaned["awfFlag"] = ident

    if "cFlag" in raw:
        # Only the presence of cFlag matters in the model; store as boolean
        cleaned["cFlag"] = bool(raw.get("cFlag") in (True, "True", "1", 1))

    if "snoozed" in raw:
        cleaned["snoozed"] = bool(raw.get("snoozed") in (True, "True", "1", 1))

    return cleaned


# Sanitize the payload for PUT. If adding a new transient ("objectDate" present), validate and reformat necessary fields.
def _sanitize_put_transient_payload(payload):
    """
    Ensure and sanitize required/optional fields for 'putTransient' endpoint,
    especially when adding a new transient.
    """
    if not isinstance(payload, dict):
        raise ValueError("Payload must be a dictionary")

    cleaned = {}

    # Required: objectName
    if "objectName" in payload:
        cleaned["objectName"] = str(payload["objectName"]).strip()
    else:
        raise ValueError("objectName is required")
    # Required: objectRa (can be string or float)
    if "objectRa" in payload: 
        cleaned["objectRa"] = str(payload["objectRa"]).strip()
    else:
        raise ValueError("objectRa is required")
    # Required: objectDec (can be string or float)
    if "objectDec" in payload:
        cleaned["objectDec"] = str(payload["objectDec"]).strip()
    else:
        raise ValueError("objectDec is required")
    # Required: objectDate (for add-new)
    if "objectDate" in payload:
        cleaned["objectDate"] = str(payload["objectDate"]).strip()
    else:
        raise ValueError("objectDate is required")
    # Required: objectSurvey
    if "objectSurvey" in payload:
        cleaned["objectSurvey"] = str(payload["objectSurvey"]).strip()
    else:
        cleaned["objectSurvey"] = ""
    # Required: objectMagnitude
    if "objectMagnitude" in payload:
        cleaned["objectMagnitude"] = str(payload["objectMagnitude"]).strip()
    else:
        raise ValueError("objectMagnitude is required")
    # Optional: objectRedshift
    if "objectRedshift" in payload:
        r = payload["objectRedshift"]
        cleaned["objectRedshift"] = str(r).strip() if r is not None and str(r).strip() != "" else "null"
    else:
        cleaned["objectRedshift"] = "null"
    # Optional: objectUrl
    if "objectUrl" in payload:
        url = payload["objectUrl"]
        cleaned["objectUrl"] = str(url).strip() if url is not None else "null"
    else:
        cleaned["objectUrl"] = "null"
    # Optional: objectImageStamp
    if "objectImageStamp" in payload:
        img = payload["objectImageStamp"]
        cleaned["objectImageStamp"] = str(img).strip() if img is not None else "null"
    else:
        cleaned["objectImageStamp"] = "null"
    # Add any other pass-through fields except ones we explicitly process
    skip_keys = set(cleaned.keys())
    for key, value in payload.items():
        if key not in skip_keys:
            cleaned[key] = value

    # Ensure ticketAuthor is set to authenticated userid
    cleaned["ticketAuthor"] = payload.get("authenticated_userid", None)

    return cleaned