from flask_jwt_extended import create_access_token, create_refresh_token
from flask_jwt_extended import get_jwt_identity
from flask_jwt_extended import jwt_required
from flask_jwt_extended import JWTManager
from flask import jsonify

from fundamentals.mysql import database, readquery
from passlib.hash import sha256_crypt

def login_user(dbConn, firstname, secondname, password, log):
    # GETTING THE DATA FROM THE DATABASE
    query = f"SELECT * from webapp_users WHERE firstname = '{firstname}' AND secondname='{secondname}'"
    rs = readquery(
        query,
        dbConn,
        log
    )
    if len(rs) <= 0:
        return jsonify({"msg": "Bad username or password", "err":"Bad username or password"}), 401
    else:
        try:
            # GETTING THE HASHED PASSWORD TO BE COMPARED WITH THE PASSED ONE
            hash_psw = rs[0]['password']
            result_check = sha256_crypt.verify(password, hash_psw)
            if result_check:
                access_token = create_access_token(identity=f"{firstname}.{secondname}")
                refresh_token = create_refresh_token(identity=f"{firstname}.{secondname}")
                return jsonify(access_token=access_token, refresh_token=refresh_token)
            else:
                return jsonify({"msg": "Bad username or password", "err":"Bad username or password"}), 401
        except Exception as e:
            print(e)
            print(traceback.format_exc())
            return jsonify({"msg": "Internal Server Error", "err":str(e)}), 505

            