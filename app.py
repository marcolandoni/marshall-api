
from fundamentals.mysql import database, readquery

from flask import Flask
from flask import jsonify
from flask import request
from flask_jwt_extended import JWTManager
from flask_jwt_extended import jwt_required
from flask_jwt_extended import get_jwt_identity, create_access_token, get_jwt

from flask_cors import CORS
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import redis
import base64
import traceback
import os
from functools import lru_cache
import logging



from models.transients.models_transients_get import models_transients_get
from models.transients.models_transients_put import models_transients_element_put
from models.transients_comments.models_transients_comments import models_transients_comments_put 
from models.transients.models_transients_count import models_transients_count
from packages.login import login_user
from packages.sanitizers import _sanitize_get_transients_request, _sanitize_patch_or_classify_request, _sanitize_comment_request, _sanitize_count_transients_request, _sanitize_put_transient_payload


logging.basicConfig(filename='/home/webserver/.config/marshall_api/marshall_api.log', level=logging.INFO)
log = logging.getLogger(__name__)


BASE_ASSETS_PATH = "/mnt/cartella_remota/transients"

asset_defs = [
      {
        "filename": "master_lightcurve.png",
        "assetDescription": "PHOT",
        "format": "png",
        "assetGroup":"PHOT"
      },
      {
        "filename": "ps1_map_color.jpeg",
        "assetDescription": "PS1",
        "format": "jpg",
        "assetGroup":"HOST"
      },
      {
        "filename": "atlas_target_stamp.jpeg",
        "assetDescription": "ATLAS",
        "format": "jpeg",
        "assetGroup":"STAMP"
      },
      {
        "filename": "ps1_target_stamp.jpeg",
        "assetDescription": "PS1 Stamp",
        "format": "jpeg",
        "assetGroup":"STAMP"
      }
    ]


@lru_cache(maxsize=2048)
def _read_b64_cached(file_path: str, mtime_ns: int, size: int) -> str:
  with open(file_path, "rb") as f:
    file_data = f.read()
  return base64.b64encode(file_data).decode("utf-8")


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
    return jsonify({"err": "Token expired", "msg":"Your token has expired. Please log in again."}), 400

# Customizing the invalid token message
@jwt.invalid_token_loader
def my_invalid_token_callback(error_string):
    return jsonify({"msg": "Your access token is not valid. Please log in again.", "err":"Token non valid."}), 400

# Customizing missing token message
@jwt.unauthorized_loader
def my_missing_token_callback(error_string):
    return jsonify({"msg": "Please obtain an access token via login to continue.", "err":"Unauthorized"}), 400


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
    if 'mwl' not in sanitized_payload and 'awl' not in sanitized_payload and 'q' not in sanitized_payload and 'snoozed' not in sanitized_payload and 'cf' not in sanitized_payload:
        return jsonify({"msg": "Please provide at least a valid Marshall Workflow location, Alert Workflow locationm query string or snoozed flag!", "err": "Invalid workflow"}), 400

    model = models_transients_get(log, sanitized_payload, db=dbConn, search=True)
    result = model.get()
    
    # Check if result is a dictionary (counts for all lists when mwl="all")
    if isinstance(result, dict):
      response = {
        "listCounts": result
      }
      return jsonify(response), 200
    
    # Otherwise, it's the standard response structure

    # TODO - Currently, the free text search works only for a single transient. In principle
    # it should work for many. This workaround (checking len of transientData) is inserted
    # for returning a consistent respond.

    qs, transientData, transientAkas, transientLightcurveData, transientAtelMatches, transients_comments, totalTicketCount, transient_history, transient_crossmatches, skyTags = result
    response ={
      "qs": qs,
      "transientData": transientData,
      "akas": transientAkas if len(transientData) > 0  else [],
      "lc_data": transientLightcurveData if len(transientData) > 0  else [],
      "ts_atel_matches": transientAtelMatches if len(transientData) > 0  else [],
      "comments": transients_comments if len(transientData) > 0  else [],
      "totalTicketCount": totalTicketCount if len(transientData) > 0  else [],
      "transient_history": transient_history if len(transientData) > 0  else [],
      "ts_xmatches": transient_crossmatches if len(transientData) > 0  else [],
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


def getSingleAsset(tbid):
  assets = {
    "HOST": [],
    "PHOT": [],
    "STAMP": []
  }
  try:
      safe_tbid = str(tbid)
      dir_path = os.path.join(BASE_ASSETS_PATH, safe_tbid)
      if os.path.isdir(dir_path):
        for ad in asset_defs:
          file_path = os.path.join(dir_path, ad["filename"])
          encoded_data = None
          try:
            st = os.stat(file_path)
            encoded_data = _read_b64_cached(file_path, st.st_mtime_ns, st.st_size)
          except FileNotFoundError:
            encoded_data = None
          except Exception:
            encoded_data = None

          assets[ad["assetGroup"]].append({
            "label": ad["assetDescription"],
            "data": encoded_data,
            "format": ad["format"]
          })
  except Exception as ex:
        print(ex)
        # Any error relative to this tbid: report as empty or log, avoid aborting on single error
        pass

  return tbid, assets


@app.route("/getAssets", methods=["POST"])
@jwt_required()
def getAssets():
  try:
    raw_payload = request.get_json(silent=True) or {}
    if not isinstance(raw_payload, dict):
      return jsonify({"msg": "Bad Request", "err": "Request body must be a JSON object"}), 400

    transient_ids = raw_payload.get("transientBucketIDs")
    if not isinstance(transient_ids, list):
      return jsonify({"msg": "Bad Request", "err": "transientBucketIDs must be a list"}), 400
    
    results = {}
    futures = []
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=20)
    for tbid in transient_ids:
      futures.append(executor.submit(getSingleAsset, tbid=tbid))

    for future in concurrent.futures.as_completed(futures):
      tbid, data = future.result()
      results[tbid] = data
      #results.append(future.result())
      
    return jsonify(results), 200
  except Exception as e:
    print(e)
    print(traceback.format_exc())
    return jsonify({
      "msg": "Bad Request",
      "err": str(traceback.format_exc())
    }), 400

@app.route("/putTransient", methods=["PUT"])
@jwt_required()
def putTransient():
  try:
    dbConn = database(
      log=log,
      dbSettings=dbSettings
    ).connect()
  except Exception as e:
    print(e)
    print(traceback.format_exc())
    return jsonify({"msg": "Bad Request", "err": str(traceback.format_exc())}), 400

  raw_payload = request.get_json(silent=True) or {}
  if not isinstance(raw_payload, dict):
    return jsonify({"msg": "Bad Request", "err": "Request body must be a JSON object"}), 400

  # adding the auth user to the request.
  raw_payload["authenticated_userid"] = get_jwt_identity()

  try:
    sanitized_payload = _sanitize_put_transient_payload(raw_payload)
  except Exception as ve:
    return jsonify({"msg": "Bad Request", "err": str(ve)}), 400
  try:
    model = models_transients_element_put(log, sanitized_payload, dbConn)
    response = model.put()
    return jsonify({"msg": response}), 200
  except Exception as e:
    print(e)
    print(traceback.format_exc())
    return jsonify({"msg": "Bad Request", "err": str(traceback.format_exc())}), 400


if __name__ == "__main__":  
  print("Starting app, listening on port 8000")
  app.run(port=8000)