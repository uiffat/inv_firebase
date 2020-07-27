import firebase_admin
from firebase_admin import credentials
from firebase_admin import db


def connect2firebase():
    # Fetch the service account key JSON file contents
    cred = credentials.Certificate('data/ngt-inv-corr-firebase-adminsdk-wj3bk-17ef3b0517.json')

    # Initialize the app with a service account, granting admin privileges
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://ngt-inv-corr.firebaseio.com/'
    })
    return True


def writeDF(ref_name, ref_df):
    ref = db.reference('ngt-inv-corr/' + ref_name)
    ref.set(ref_df.to_json())
    return True


if __name__ == "__main __":
    pass
