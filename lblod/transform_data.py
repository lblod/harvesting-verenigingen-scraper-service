import copy
import os
import json
import uuid
from functools import lru_cache
from helpers import logger

def create_uuid_from_string(input_string):
    if input_string:
        generated_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, input_string)
        return generated_uuid
    return ""


def is_valid_association_data(item):
    """
    Validates an item and returns a tuple (is_valid, reason).

    Returns:
        (True, None) if the item is valid and should be processed
        (False, reason_string) if the item is invalid and should be skipped
    """
    v_code = item.get("vCode", "unknown")

    # Check for removed resources or unexpected responses. i.e. 404 or 502 responses
    if item.get("type") in ["RemovedResource", "UnexpectedResponse"]:
        return (False, f"Found a {item['type']} for {v_code}")

    # Check for missing or empty location
    if 'locaties' not in item or not item.get('locaties'):
        return (False, f"Vereniging {v_code} has no location")

    # Check if all location postal codes are outside Flanders or Brussels
    locaties = item.get("locaties", [])
    if locaties and all(
        not is_postal_code_in_flanders_or_brussels(locatie.get("adres", {}).get("postcode"))
        for locatie in locaties
    ):
        return (False, f"Vereniging {v_code} has no locations in Flanders or Brussels")

    # Check for "Dubbel" status
    if "status" in item:
        formatted_status = item["status"].strip().lower()
        if formatted_status == "dubbel":
            return (False, f"Vereniging {v_code} is marked as 'Dubbel'")

    # Check if association is opted out of public data stream
    if item.get("isUitgeschrevenUitPubliekeDatastroom") is True:
        return (False, f"Vereniging {v_code} is opted out of public data stream")

    return (True, None)


def transform_data(data):
    current_directory = os.path.dirname(os.path.realpath(__file__))
    json_file_path = os.path.join(current_directory, "types.json")
    with open(json_file_path, "r") as file:
        association_types = json.load(file)
    transformed_data = []


    def create_location(locatie):
        # pull the address-register URI if available
        verwijst_naar_id = locatie.get("verwijstNaar", {}).get("@id")

        # build the address representation
        bestaat_uit = {
            **locatie.get("adres", {}),
            "adresvoorstelling": locatie.get("adresvoorstelling", ""),
        }

        # add adres:verwijstNaar as a linked @id if present
        if verwijst_naar_id:
            bestaat_uit["address:verwijstNaar"] = {"@id": verwijst_naar_id}

        return {
            "@id": locatie.get("@id", ""),
            "locatieId": locatie.get("locatieId", ""),
            "@type": locatie.get("@type", ""),
            "description": locatie.get("naam", ""),
            "locatieType": {
                "@id": "con:"
                + str(create_uuid_from_string(locatie.get("locatietype", ""))),
                "@type": "concept:TypeVestiging",
                "naam": locatie.get("locatietype", ""),
            },
            "bestaatUit": bestaat_uit,
        }

    def create_contact_point(contact):
        new_contact = {
            "@id": contact.get("@id", ""),
            "contactgegevenId": contact.get("contactgegevenId", ""),
            "@type": contact.get("@type", ""),
            "contactgegeventype": contact["contactgegeventype"],
        }
        if contact["isPrimair"]:
            new_contact["primairContact"] = "Primary"
        else:
            new_contact["primairContact"] = "Secondary"
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
        }
        if "telefoon" in contact:
            new_contact["telefoon"] = contact["telefoon"]
        if "e-mail" in contact:
            new_contact["email"] = contact["e-mail"]
        if "socialMedia" in contact:
            new_contact["website"] = contact["socialMedia"]
        return new_contact

    def create_representative(representative_data, v_code):
        new_representative = {
            "@id": f"lidmaatschap:{create_uuid_from_string(v_code + '_' + str(representative_data.get('vertegenwoordigerId')))}",
            "vertegenwoordigerId": representative_data.get("vertegenwoordigerId", ""),
            "@type": "org:Membership",
            "vertegenwoordigerPersoon": {
                "@id": representative_data.get("@id", ""),
                "@type": representative_data.get("@type", ""),
                "voornaam": representative_data.get("voornaam", ""),
                "achternaam": representative_data.get("achternaam", ""),
                "contactgegevens": [],
            },
        }

        if representative_data.get("isPrimair", False):
            new_representative["primaireVertegenwoordiger"] = { "@id": "lblodconcept:75e74415-35cf-4da5-bac5-b72a1c137799" }
        else:
            new_representative["primaireVertegenwoordiger"] = { "@id": "lblodconcept:78451ac5-ec0b-469d-b918-0a8ef92a77b2" }

        contact_info = representative_data.get("vertegenwoordigerContactgegevens", [])
        if contact_info:
            new_representative["vertegenwoordigerPersoon"]["contactgegevens"].append(
                create_contact_representative(contact_info)
            )
        return new_representative

    for item in data:
        is_valid, invalid_reason = is_valid_association_data(item)
        if not is_valid:
            logger.info(f"{invalid_reason}. Skipping")
            continue

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
                verenigingstype = vereniging.get(
                    "verenigingstype", {}
                )  # Use get() with a default empty dictionary
                if "code" in verenigingstype and assoc_type[
                    "code"
                ] == verenigingstype.get("code", ""):
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
                vertegenwoordigers.append(
                    create_representative(vertegenwoordiger, v_code)
                )

        # STATUS MAPPING
        if "status" in item:
            formatted_status = item["status"].strip().lower()
            if formatted_status == "actief":
                status = {
                    "@id": "http://lblod.data.gift/concepts/63cc561de9188d64ba5840a42ae8f0d6"
                }
            elif formatted_status == "niet actief":
                status = {
                    "@id": "http://lblod.data.gift/concepts/d02c4e12bf88d2fdf5123b07f29c9311"
                }
            elif formatted_status == "in oprichting":
                status = {
                    "@id": "http://lblod.data.gift/concepts/abf4fee82019f88cf122f986830621ab"
                }
            elif formatted_status == "gestopt":
                status = {
                    "@id": "http://lblod.data.gift/concepts/3d790fd9-bec9-43dd-840c-f835eda6997e"
                }

        if not primary_location:
            for locatie in locaties:
                if (
                    locatie.get("locatieType", {}).get("naam")
                    == "Maatschappelijke zetel volgens KBO"
                ):
                    primary_location = locatie
                    break

            if not primary_location:
                for locatie in locaties:
                    if locatie.get("locatieType", {}).get("naam") == "Correspondentie":
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
        vereniging["datumLaatsteAanpassing"] = vereniging.get("metadata", {}).get(
            "datumLaatsteAanpassing"
        )
        if status:
            vereniging["status"] = status
        transformed_data.append(vereniging)
    return transformed_data




@lru_cache(maxsize=1)
def _load_postal_codes():
    """Load postal codes from JSON file and cache the result."""
    current_directory = os.path.dirname(os.path.realpath(__file__))
    json_file_path = os.path.join(current_directory, "postal_codes.json")

    with open(json_file_path, "r") as file:
        postal_codes_data = json.load(file)

    # Create a set for O(1) lookup performance
    valid_postal_codes = set(
        postal_codes_data.get("postal_codes_brussels", []) +
        postal_codes_data.get("postal_codes_flanders", [])
    )
    return valid_postal_codes


def is_postal_code_in_flanders_or_brussels(postal_code):
    # Normalize postal_code to string for comparison
    postal_code_str = str(postal_code).strip()

    # Check if postal code is in the cached set
    valid_postal_codes = _load_postal_codes()
    return postal_code_str in valid_postal_codes