import logging
import re

from fundamentals.mysql import database, readquery

from packages.login import login_user

from flask import Flask
from flask import jsonify
from flask import request
from flask_jwt_extended import JWTManager
from flask_jwt_extended import jwt_required
from flask_jwt_extended import get_jwt_identity, create_access_token, get_jwt
from flask_cors import CORS
from datetime import timedelta

import redis

from models.transients.models_transients_get import models_transients_get
from models.transients.models_transients_put import models_transients_element_put
from models.transients_comments.models_transients_comments import models_transients_comments_put 
from models.transients.models_transients_count import models_transients_count

import traceback

logging.basicConfig(filename='/home/webserver/.config/marshall_api/marshall_api.log', level=logging.INFO)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
#  Input sanitization helpers
# ---------------------------------------------------------------------------

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
    "snoozed"
}

ALLOWED_AWL_VALUES = {
    "queued for atel",
    "soxs classification released",
    "archived without alert",
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
    q = raw.get("q")
    if q is not None:
        cleaned["q"] = re.sub(r"[^A-Za-z0-9]", "", str(q))[:100]

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
    if mwl and awl:
        return None #only once at time
    cf = raw.get("cf")
    if cf is not None:
        # classifiedFlag is stored as "1"/"0" in SQL
        cleaned["cf"] = "1" if str(cf) in ("1", "true", "True", "yes") else "0"

    if "snoozed" in raw:
        snoozed = raw.get("snoozed")
        cleaned["snoozed"] = bool(snoozed in (True, "True", "1", 1))

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
        # If both are provided, define your consistency rule here.
        # This is an example: you may want to replace or expand the logic.
        # For example, let's make them mutually exclusive:
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


dbSettings = {
    'host': '192.167.39.99', 
    'user': 'marshall', 
    'password': 'mar5ha11', 
    'db': 'marshall'
}



app = Flask(__name__)
cors = CORS(app, resources={r"*": {"origins": "*"}})
ACCESS_EXPIRES = timedelta(hours=24)
app.config["JWT_SECRET_KEY"] = "super-secret"  # Change this!
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(hours=25)
app.config["JWT_REFRESH_TOKEN_EXPIRES"] = timedelta(hours=72)
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = ACCESS_EXPIRES


jwt = JWTManager(app)

# ADDING CUSTOMIZED ERROR MESSAGES FOR JWT

# Customizing the expired token message
@jwt.expired_token_loader
def my_expired_token_callback(jwt_header, jwt_payload):
    return jsonify({"msg": "Token expired", "err":"Your token has expired. Please log in again."}), 400

# Customizing the invalid token message
@jwt.invalid_token_loader
def my_invalid_token_callback(error_string):
    return jsonify({"msg": "Token not valid", "err":"Your access token is not valid. Please log in again."}), 400

# Customizing missing token message
@jwt.unauthorized_loader
def my_missing_token_callback(error_string):
    return jsonify({"msg": "Unauthorized", "err":"Please obtain an access token via login to continue."}), 400


jwt_redis_blocklist = redis.StrictRedis(
    host="localhost", port=6379, db=0, decode_responses=True
)

@jwt.token_in_blocklist_loader
def check_if_token_is_revoked(jwt_header, jwt_payload: dict):
    jti = jwt_payload["jti"]
    token_in_redis = jwt_redis_blocklist.get(jti)
    return token_in_redis is not None

@app.route("/login", methods=["POST"])
def login():
  try:
    dbConn = database(
      log=log,
      dbSettings=dbSettings
    ).connect()
    firstname = request.json.get("firstname", None)
    lastname = request.json.get("lastname", None)
    password = request.json.get("password", None)

    return login_user(dbConn, firstname, lastname, password, log)
  except:
    return jsonify({"msg": "Bad request", "err":"Bad request"}), 400


@app.route("/refresh", methods=["POST"])
@jwt_required(refresh=True)
def refresh():
  try:
    identity = get_jwt_identity()
    access_token = create_access_token(identity=identity, fresh=False)
    return jsonify(access_token=access_token)
  except:
    return jsonify({"msg": "Bad request", "err":"Bad request"}), 400


@app.route("/logged_user", methods=["GET"])
@jwt_required()
def logged_user():
    # Access the identity of the current user with get_jwt_identity
    try:
      current_user = get_jwt_identity()
      print(current_user)
      return jsonify(logged_in_as=current_user), 200
    except Exception as e:
      return jsonify({"msg": "Bad request", "err":"Bad request"}), 400


@app.route("/logout", methods=["DELETE"])
@jwt_required(verify_type=False)
def logout():
  try:
      token = get_jwt()
      jti = token["jti"]
      ttype = token["type"]
      jwt_redis_blocklist.set(jti, "", ex=ACCESS_EXPIRES)

      # Returns "Access token revoked" or "Refresh token revoked"
      return jsonify(msg=f"{ttype.capitalize()} token successfully revoked")
  except Exception as e:
    return jsonify({"msg": "Bad request", "err":"Bad request"}), 400
    print(e)

# GENERIC APP ROUTE FOR GETTING TRANSIENTS DATA

@app.route("/getTransients", methods=["POST"])
@jwt_required()
def getTransients():
  try:
    dbConn = database(
      log=log,
      dbSettings=dbSettings
    ).connect()

    raw_payload = request.get_json(silent=True) or {}
    if not isinstance(raw_payload, dict):
      return jsonify({"msg": "Bad Request", "err": "Request body must be a JSON object"}), 400

    sanitized_payload = _sanitize_get_transients_request(raw_payload)
    print(sanitized_payload)
    if not sanitized_payload:
        return jsonify({"msg": "Bad Request", "err": "Please provide a valid request."}), 400
    if 'mwl' not in sanitized_payload and 'awl' not in sanitized_payload:
        return jsonify({"msg": "Please provide at least a valid Marshall Workflow location or Alert Workflow location", "err": "Invalid workflow"}), 400

    model = models_transients_get(log, sanitized_payload, db=dbConn, search=True)
    result = model.get()
    
    # Check if result is a dictionary (counts for all lists when mwl="all")
    if isinstance(result, dict):
      response = {
        "listCounts": result
      }
      return jsonify(response), 200
    
    # Otherwise, it's the standard response structure
    qs, transientData, transientAkas, transientLightcurveData, transientAtelMatches, transients_comments, totalTicketCount, transient_history, transient_crossmatches, skyTags = result
    response ={
      "qs": qs,
      "transientData": transientData,
      "akas": transientAkas,
      "lc_data": transientLightcurveData,
      "ts_atel_matches": transientAtelMatches,
      "comments": transients_comments,
      "totalTicketCount": totalTicketCount,
      "transient_history": transient_history,
      "ts_xmatches": transient_crossmatches,
      "ts_skytag": skyTags

    }
    print("returning ", str(len(transientData)))
    return jsonify(response), 200
  except Exception as e:
    print(e)
    print(traceback.format_exc())
    return jsonify({"msg": "Bad Request", "err": str(traceback.format_exc())}), 400

# GENERIC APP ROUTE FOR PATCHING TRANSIENTS DATA (POSSIBLY CLASSIFICATION WILL GO UNDER THIS ROUTE)
@app.route("/patchTransient", methods=["PATCH"])
@jwt_required()
def patchTransient():
  try:
    dbConn = database(
      log=log,
      dbSettings=dbSettings
    ).connect()

    raw_payload = request.get_json(silent=True) or {}
    if not isinstance(raw_payload, dict):
      return jsonify({"msg": "Bad Request", "err": "Request body must be a JSON object"}), 400

    # adding the auth user to the request.
    raw_payload["authenticated_userid"] = get_jwt_identity()

    try:
      sanitized_payload = _sanitize_patch_or_classify_request(raw_payload)
    except ValueError as ve:
      return jsonify({"msg": "Bad Request", "err": str(ve)}), 400

    model = models_transients_element_put(log, sanitized_payload, dbConn)
    response = model.put()
    return jsonify({"msg": response}), 200
  except Exception as e:
    print(e)
    print(traceback.format_exc())
    return jsonify({"msg": "Bad Request", "err": str(traceback.format_exc())}), 400

# CLASSIFY TARGET ROUTE
@app.route("/classifyTransient", methods=["PATCH"])
@jwt_required()
def classifyTransient():
  try:
    dbConn = database(
      log=log,
      dbSettings=dbSettings
    ).connect()

    raw_payload = request.get_json(silent=True) or {}
    if not isinstance(raw_payload, dict):
      return jsonify({"msg": "Bad Request", "err": "Request body must be a JSON object"}), 400

    # adding the auth user to the request.
    raw_payload["authenticated_userid"] = get_jwt_identity()

    try:
      sanitized_payload = _sanitize_patch_or_classify_request(raw_payload)
    except ValueError as ve:
      return jsonify({"msg": "Bad Request", "err": str(ve)}), 400

    model = models_transients_element_put(log, sanitized_payload, dbConn)
    response = model.put()
    return jsonify({"msg": response}), 200
  except Exception as e:
    print(e)
    print(traceback.format_exc())
    return jsonify({"msg": "Bad Request", "err": str(traceback.format_exc())}), 400

@app.route("/putComment", methods=["PUT"])
@jwt_required()
def putComment():
  try:
    dbConn = database(
      log=log,
      dbSettings=dbSettings
    ).connect()

    raw_payload = request.get_json(silent=True) or {}
    if not isinstance(raw_payload, dict):
      return jsonify({"msg": "Bad Request", "err": "Request body must be a JSON object"}), 400

    # adding the auth user to the request.
    raw_payload["authenticated_userid"] = get_jwt_identity()

    try:
      sanitized_payload = _sanitize_comment_request(raw_payload)
    except ValueError as ve:
      return jsonify({"msg": "Bad Request", "err": str(ve)}), 400

    model = models_transients_comments_put(log, sanitized_payload, dbConn)
    response = model.put()
    return jsonify({"msg": response}), 200
  except Exception as e:
    print(e)
    print(traceback.format_exc())
    return jsonify({"msg": "Bad Request", "err": str(traceback.format_exc())}), 400

@app.route("/countTransients", methods=["POST"])
@jwt_required()
def countTransients():
  try:
    dbConn = database(
      log=log,
      dbSettings=dbSettings
    ).connect()

    raw_payload = request.get_json(silent=True) or {}
    if not isinstance(raw_payload, dict):
      return jsonify({"msg": "Bad Request", "err": "Request body must be a JSON object"}), 400

    sanitized_payload = _sanitize_count_transients_request(raw_payload)
    print(sanitized_payload)

    model = models_transients_count(log, sanitized_payload, db=dbConn)
    if len(sanitized_payload) > 0:
      count = model.get()
    else: 
      raise Exception("bad request! Please check the provided flags and format.")
    return jsonify(count=count), 200
  except Exception as err:
    return jsonify({"msg": "Bad Request", "err": str(traceback.format_exc())}), 400

if __name__ == "__main__":  
  print("Starting app, listening on port 8000")
  app.run(port=8000)