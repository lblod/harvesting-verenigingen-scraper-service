import copy
import os
import json
import uuid
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def create_uuid_from_string(input_string):
    if input_string:
        generated_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, input_string)
        logging.debug(f"Generated UUID: {generated_uuid} for input string: {input_string}")
        return generated_uuid
    logging.warning("Input string is empty, returning empty UUID")
    return ""

def transform_data(data):
    current_directory = os.path.dirname(os.path.realpath(__file__))
    json_file_path = os.path.join(current_directory, "types.json")
    logging.debug(f"Loading JSON file from path: {json_file_path}")

    try:
        with open(json_file_path, "r") as file:
            association_types = json.load(file)
    except json.JSONDecodeError:
        logging.error(f"Error parsing JSON from {json_file_path}")
        raise ValueError(f"Error parsing JSON from {json_file_path}")
    logging.debug(f"Association types loaded: {association_types}")

    transformed_data = []

    def create_location(locatie):
        if locatie is None:
            logging.error("Location is None")
            raise ValueError("locatie is None")
        location_id = locatie.get("@id", "")
        location_type = locatie.get("@type", "")
        location_name = locatie.get("naam", "")
        locatietype_id = "con:" + str(create_uuid_from_string(locatie.get("locatietype", "")))
        locatie_type_name = locatie.get("locatietype", "")
        locatie_address = {**locatie.get("adres", {}), "adresvoorstelling": locatie.get("adresvoorstelling", "")}

        location = {
            "@id": location_id,
            "@type": location_type,
            "description": location_name,
            "locatieType": {
                "@id": locatietype_id,
                "@type": "concept:TypeVestiging",
                "naam": locatie_type_name,
            },
            "bestaatUit": locatie_address
        }
        logging.debug(f"Created location: {location}")
        return location

    def create_contact_point(contact):
        if contact is None:
            logging.error("Contact is None")
            raise ValueError("contact is None")

        contact_id = contact.get("@id", "")
        contact_type = contact.get("@type", "")
        contact_type_value = contact.get("contactgegeventype", "")
        contact_value = contact.get("waarde", "")
        new_contact = {
            "@id": contact_id,
            "@type": contact_type,
            "contactgegeventype": contact_type_value,
        }
        if contact.get("isPrimair"):
            new_contact["primairContact"] = "Primary"
        if contact_type_value == "Telefoon":
            new_contact["telefoon"] = contact_value
        elif contact_type_value == "E-mail":
            new_contact["email"] = contact_value
        elif contact_type_value in ["Website", "SocialMedia"]:
            new_contact["website"] = contact_value

        logging.debug(f"Created contact point: {new_contact}")
        return new_contact

    def create_contact_representative(contact):
        if contact is None:
            logging.error("Contact is None")
            raise ValueError("contact is None")

        representative_id = contact.get("@id", "")
        representative_type = contact.get("@type", "")
        contact_phone = contact.get("telefoon", "")
        contact_email = contact.get("e-mail", "")
        contact_social_media = contact.get("socialMedia", "")

        new_contact = {
            "@id": representative_id,
            "@type": representative_type,
            "primairContact": "Primary" if contact.get("isPrimair", False) else "Secondary"
        }
        if contact_phone:
            new_contact["telefoon"] = contact_phone
        if contact_email:
            new_contact["email"] = contact_email
        if contact_social_media:
            new_contact["website"] = contact_social_media

        logging.debug(f"Created contact representative: {new_contact}")
        return new_contact

    def create_representative(representative_data, v_code):
        if representative_data is None:
            logging.error("Representative data is None")
            raise ValueError("representative_data is None")

        representative_id = f"lidmaatschap:{create_uuid_from_string(v_code + '_' + str(representative_data.get('vertegenwoordigerId', '')))}"
        representative = {
            "@id": representative_id,
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
            representative["vertegenwoordigerPersoon"]["contactgegevens"] = [
                create_contact_representative(info) for info in contact_info
            ]

        logging.debug(f"Created representative: {representative}")
        return representative

    for item in data:
        if item is None:
            logging.error("Item in data is None")
            raise ValueError("item in data is None")

        vereniging = copy.deepcopy(item)
        v_code = vereniging.get("vCode", "")
        primary_location = None
        locaties = []
        contact_gegevens = []
        vertegenwoordigers = []

        logging.debug(f"Processing item: {vereniging}")

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
        # for vertegenwoordiger in item.get("vertegenwoordigers", []):
        #     vertegenwoordigers.append(create_representative(vertegenwoordiger, v_code))

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

        logging.debug(f"Transformed vereniging: {vereniging}")

    return transformed_data
