# config.py

FINAL_COLUMNS = [
    "external_id",
    "first_name",
    "last_name",
    "dob",
    "email",
    "phone",
    "partner_code"
]

PARTNER_CONFIG = {
    "acme": {
        "file_name": "acme.txt",
        "file": {
            "delimiter": "|",
            "header": True
        },
        "column_mapping": {
            "MBI": "external_id",
            "FNAME": "first_name",
            "LNAME": "last_name",
            "DOB": "dob",
            "EMAIL": "email",
            "PHONE": "phone"
        },
        "partner_code": "ACME"
    },

    "bettercare": {
        "file_name": "bettercare.csv",
        "file": {
            "delimiter": ",",
            "header": True
        },
        "column_mapping": {
            "subscriber_id": "external_id",
            "first_name": "first_name",
            "last_name": "last_name",
            "date_of_birth": "dob",
            "email": "email",
            "phone": "phone"
        },
        "partner_code": "BETTERCARE"
    },

    "careplus": {
        "file_name": "careplus.txt",
        "file": {
            "delimiter": "^",
            "header": True
        },
        "column_mapping": {
            "member_no": "external_id",
            "given_name": "first_name",
            "surname": "last_name",
            "birthdate": "dob",
            "email_address": "email",
            "contact_number": "phone"
        },
        "partner_code": "CAREPLUS"
    }

}
