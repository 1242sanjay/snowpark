# ALTER USER snowpark_user SET RSA_PUBLIC_KEY='MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQ...'
from snowflake.snowpark import Session
# pip install cryptography
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

# load the private rsa key
with open("../../rsa-key/rsa_key.p8", "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None,
        backend=default_backend()
    )

# private key text
private_key_text = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

# print the private key text
print(private_key_text)

# rsa key based authentication
def snowpark_key_auth() -> Session:
    # create a session object
    session = Session.builder.configs({
        "account": "<account>",
        "user": "1242sanjay",
        "private_key": private_key_text
    }).create()
    return session

if __name__ == "__main__":
    # create a session object
    session = snowpark_key_auth()
    print(session)
    print(session.sql("select current_session()").collect())
    # close the session
    session.close()