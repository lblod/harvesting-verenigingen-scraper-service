import copy
import os
import json
import uuid


def create_uuid_from_string(input_string):
    if input_string:
        generated_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, input_string)
        return generated_uuid
    return ""


def transform_data(data):
    current_directory = os.path.dirname(os.path.realpath(__file__))
    json_file_path = os.path.join(current_directory, "types.json")
    with open(json_file_path, "r") as file:
        association_types = json.load(file)
    transformed_data = []

    def create_location(locatie):
            return {
                "@id": locatie.get("@id", ""),
                "@type": locatie.get("@type", ""),
                "description": locatie.get("naam", "") ,
                "locatieType": {
                    "@id": "con:" + str(create_uuid_from_string(locatie.get("locatietype", ""))),
                    "@type": "concept:TypeVestiging",
                    "naam": locatie.get("locatietype", ""),
                },
                "bestaatUit": {**locatie.get("adres", {}), "adresvoorstelling": locatie.get("adresvoorstelling", "")}
            }

    def create_contact_point(contact):
        new_contact = {
            "@id": contact.get("@id", ""),
            "@type": contact.get("@type", ""),
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

    def create_contact_representative(contact):
        new_contact = {
            "@id": contact.get("@id", ""),
            "@type": contact.get("@type", ""),
            "primairContact": "Primary" if contact.get("isPrimair", False) else "Secondary"
        }
        if "telefoon" in contact:
            new_contact["telefoon"] = contact["telefoon"]
        if "e-mail" in contact:
            new_contact["email"] = contact["e-mail"]
        if "socialMedia" in contact:
            new_contact["website"] = contact["socialMedia"]
        return new_contact

    def create_representative(representative_data ,v_code):
        new_representative = {
            "@id":  f"lidmaatschap:{create_uuid_from_string(v_code + '_' + str(representative_data.get('vertegenwoordigerId')))}",
            "@type": "org:Membership",
            "vertegenwoordigerPersoon": {
                "@id": representative_data.get("@id", ""),
                "@type": representative_data.get("@type", ""),
                "voornaam": representative_data.get("voornaam", ""),
                "achternaam": representative_data.get("achternaam", ""),
                "contactgegevens": []
            },
        }
        contact_info = representative_data.get("vertegenwoordigerContactgegevens", [])
        if contact_info:
            new_representative["vertegenwoordigerPersoon"]["contactgegevens"].append(create_contact_representative(contact_info))
        return new_representative

    for item in data:
        vereniging = copy.deepcopy(item)
        v_code = vereniging.get("vCode", "")
        primary_location = None
        locaties = []
        contact_gegevens = []
        vertegenwoordigers = []
        status = None

        # ASSOCIATION TYPES
        for assoc_type in association_types:
            if "code" in assoc_type and "@id" in assoc_type:
                verenigingstype = vereniging.get("verenigingstype", {})  # Use get() with a default empty dictionary
                if "code" in verenigingstype and assoc_type["code"] == verenigingstype.get("code", ""):
                    verenigingstype["@id"] = assoc_type.get("@id", "")
                    vereniging["verenigingstype"] = verenigingstype

        # IDENTIFIERS
        for sleutel in vereniging["sleutels"]:
            if "codeerSysteem" in sleutel:
                if sleutel["codeerSysteem"] == "Vcode":
                    sleutel["codeerSysteem"] = "vCode"

        # LOCATIES
        for locatie in item["locaties"]:
            if "isPrimair" in locatie and locatie["isPrimair"]:
                primary_location = create_location(locatie)
            else:
                locaties.append(create_location(locatie))

        # CONTACTGEGEVENS
        if "contactgegevens" in item and item["contactgegevens"]:
             for contact in item["contactgegevens"]:
                contact_gegevens.append(create_contact_point(contact))

        # VERTEGENWOORDIGERS
        if "vertegenwoordigers" in item and item["vertegenwoordigers"]:
            for vertegenwoordiger in item["vertegenwoordigers"]:
                vertegenwoordigers.append(create_representative(vertegenwoordiger, v_code))

        # STATUS MAPPING
        if "status" in item:
            formattedStatus = item["status"].strip().lower()
            if formattedStatus == "actief":
                status = { "@id": "http://lblod.data.gift/concepts/63cc561de9188d64ba5840a42ae8f0d6" }
            elif formattedStatus == "niet actief":
                status = { "@id": "http://lblod.data.gift/concepts/d02c4e12bf88d2fdf5123b07f29c9311" }
            elif formattedStatus == "in oprichting":
                status = { "@id": "http://lblod.data.gift/concepts/abf4fee82019f88cf122f986830621ab" }

        if not primary_location:
            for locatie in locaties:
                if locatie.get('locatieType', {}).get('naam') == "Maatschappelijke zetel volgens KBO":
                    primary_location = locatie
                    break

            if not primary_location:
                for locatie in locaties:
                    if locatie.get('locatieType', {}).get('naam') == "Correspondentie":
                        primary_location = locatie
                        break

                if not primary_location:
                    primary_location = locaties[0] if locaties else None

        if primary_location and primary_location in locaties:
            locaties.remove(primary_location)

        vereniging["primaireLocatie"] = primary_location
        vereniging["locaties"] = locaties
        vereniging["contactgegevens"] = contact_gegevens
        vereniging["vertegenwoordigers"] = vertegenwoordigers
        vereniging["@type"] = "fei:Vereniging"
        vereniging["datumLaatsteAanpassing"] = vereniging.get("metadata", {}).get("datumLaatsteAanpassing")
        if status:
            vereniging["status"] = status
        transformed_data.append(vereniging)
    return transformed_data
