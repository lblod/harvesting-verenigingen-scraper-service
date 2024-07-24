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
        try:
            association_types = json.load(file)
        except json.JSONDecodeError:
            raise ValueError(f"Error parsing JSON from {json_file_path}")
    transformed_data = []

    def create_location(locatie):
        if locatie is None:
            raise ValueError("locatie is None")
        return {
            "@id": locatie.get("@id", ""),
            "@type": locatie.get("@type", ""),
            "description": locatie.get("naam", ""),
            "locatieType": {
                "@id": "con:" + str(create_uuid_from_string(locatie.get("locatietype", ""))),
                "@type": "concept:TypeVestiging",
                "naam": locatie.get("locatietype", ""),
            },
            "bestaatUit": {**locatie.get("adres", {}), "adresvoorstelling": locatie.get("adresvoorstelling", "")}
        }

    def create_contact_point(contact):
        if contact is None:
            raise ValueError("contact is None")
        new_contact = {
            "@id": contact.get("@id", ""),
            "@type": contact.get("@type", ""),
            "contactgegeventype": contact.get("contactgegeventype", ""),
        }
        if contact.get("isPrimair"):
            new_contact["primairContact"] = "Primary"
        if contact.get("contactgegeventype") == "Telefoon":
            new_contact["telefoon"] = contact.get("waarde", "")
        if contact.get("contactgegeventype") == "E-mail":
            new_contact["email"] = contact.get("waarde", "")
        if contact.get("contactgegeventype") in ["Website", "SocialMedia"]:
            new_contact["website"] = contact.get("waarde", "")
        return new_contact

    def create_contact_representative(contact):
        if contact is None:
            raise ValueError("contact is None")
        new_contact = {
            "@id": contact.get("@id", ""),
            "@type": contact.get("@type", ""),
            "primairContact": "Primary" if contact.get("isPrimair", False) else "Secondary"
        }
        if "telefoon" in contact:
            new_contact["telefoon"] = contact.get("telefoon", "")
        if "e-mail" in contact:
            new_contact["email"] = contact.get("e-mail", "")
        if "socialMedia" in contact:
            new_contact["website"] = contact.get("socialMedia", "")
        return new_contact

    def create_representative(representative_data, v_code):
        if representative_data is None:
            raise ValueError("representative_data is None")
        new_representative = {
            "@id": f"lidmaatschap:{create_uuid_from_string(v_code + '_' + str(representative_data.get('vertegenwoordigerId', '')))}",
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
            new_representative["vertegenwoordigerPersoon"]["contactgegevens"] = [
                create_contact_representative(info) for info in contact_info
            ]
        return new_representative

    for item in data:
        if item is None:
            raise ValueError("item in data is None")

        vereniging = copy.deepcopy(item)
        v_code = vereniging.get("vCode", "")
        primary_location = None
        locaties = []
        contact_gegevens = []
        vertegenwoordigers = []

        # ASSOCIATION TYPES
        for assoc_type in association_types:
            if "code" in assoc_type and "@id" in assoc_type:
                verenigingstype = vereniging.get("verenigingstype", {})
                if "code" in verenigingstype and assoc_type["code"] == verenigingstype.get("code", ""):
                    verenigingstype["@id"] = assoc_type.get("@id", "")
                    vereniging["verenigingstype"] = verenigingstype

        # IDENTIFIERS
        for sleutel in vereniging.get("sleutels", []):
            if "codeerSysteem" in sleutel:
                if sleutel["codeerSysteem"] == "Vcode":
                    sleutel["codeerSysteem"] = "vCode"

        # LOCATIES
        for locatie in item.get("locaties", []):
            if "isPrimair" in locatie and locatie["isPrimair"]:
                primary_location = create_location(locatie)
            else:
                locaties.append(create_location(locatie))

        # CONTACTGEGEVENS
        for contact in item.get("contactgegevens", []):
            contact_gegevens.append(create_contact_point(contact))

        # VERTEGENWOORDIGERS
        for vertegenwoordiger in item.get("vertegenwoordigers", []):
            vertegenwoordigers.append(create_representative(vertegenwoordiger, v_code))

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
        vereniging["@type"] = "fei:FeitelijkeVereniging"
        vereniging["datumLaatsteAanpassing"] = vereniging.get("metadata", {}).get("datumLaatsteAanpassing")
        transformed_data.append(vereniging)
    return transformed_data
