import copy
import os
import json
import uuid


def create_uuid_from_string(input_string):
    generated_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, input_string)
    return generated_uuid


def transform_data(data):
    current_directory = os.path.dirname(os.path.realpath(__file__))
    json_file_path = os.path.join(current_directory, "types.json")
    with open(json_file_path, "r") as file:
        association_types = json.load(file)
    transformed_data = []

    def create_location(locatie):
        return {
            "@id": locatie["@id"],
            "@type": locatie["@type"],
            "description": locatie["naam"],
            "locatieType": {
                "@id": "con:" + str(create_uuid_from_string(locatie["locatietype"])),
                "@type": "concept:TypeVestiging",
                "naam": locatie["locatietype"],
            },
            "bestaatUit": locatie["adres"],
        }

    def create_contact_point(contact):
        new_contact = {
            "@id": contact["@id"],
            "@type": contact["@type"],
            "contactgegeventype": contact["contactgegeventype"],
        }
        if contact["isPrimair"]:
            new_contact["primairContact"] = "Primary"
        if contact["contactgegeventype"] == "Telefoon":
            new_contact["telefoon"] = contact["waarde"]
        if contact["contactgegeventype"] == "E-mail":
            new_contact["email"] = contact["waarde"]
        if (
            contact["contactgegeventype"] == "Website"
            or contact["contactgegeventype"] == "SocialMedia"
        ):
            new_contact["website"] = contact["waarde"]
        return new_contact

    for item in data:
        vereniging = copy.deepcopy(item)
        primary_location = None
        locaties = []
        contact_gegevens = []
        for type in association_types:
            if type["code"] == vereniging["verenigingstype"]["code"]:
                vereniging["verenigingstype"]["@id"] = type["@id"]

        for sleutel in vereniging["sleutels"]:
            if sleutel["codeerSysteem"] == "Vcode":
                sleutel["codeerSysteem"] = "vCode"

        for locatie in item["locaties"]:
            if locatie["isPrimair"]:
                primary_location = create_location(locatie)
            else:
                locaties.append(create_location(locatie))

        for contact in item["contactgegevens"]:
            contact_gegevens.append(create_contact_point(contact))

        vereniging["primaireLocatie"] = primary_location
        vereniging["locaties"] = locaties
        vereniging["contactgegevens"] = contact_gegevens
        transformed_data.append(vereniging)

    return transformed_data
