"""
Test script for transform_data function from lblod/transform_data.py
"""
import json
from lblod.transform_data import transform_data


def test_transform_data_basic():
    """Test transform_data with basic test data"""

    # Sample test data mimicking the structure expected by transform_data
    test_data = [
        {
            "vCode": "V0001234",
            "naam": "Test Vereniging",
            "verenigingstype": {
                "code": "FWB"
            },
            "sleutels": [
                {
                    "codeerSysteem": "Vcode",
                    "waarde": "V0001234"
                }
            ],
            "locaties": [
                {
                    "@id": "loc1",
                    "locatieId": "12345",
                    "@type": "Locatie",
                    "naam": "Hoofdzetel",
                    "locatietype": "Maatschappelijke zetel volgens KBO",
                    "isPrimair": True,
                    "adres": {
                        "straatnaam": "Teststraat",
                        "huisnummer": "1",
                        "postcode": "1000",
                        "gemeente": "Brussel"
                    },
                    "adresvoorstelling": "Teststraat 1, 1000 Brussel",
                    "verwijstNaar": {
                        "@id": "https://data.vlaanderen.be/id/adres/123456"
                    }
                }
            ],
            "contactgegevens": [
                {
                    "@id": "contact1",
                    "contactgegevenId": "c1",
                    "@type": "Contactgegeven",
                    "contactgegeventype": "E-mail",
                    "waarde": "test@example.com",
                    "isPrimair": True
                },
                {
                    "@id": "contact2",
                    "contactgegevenId": "c2",
                    "@type": "Contactgegeven",
                    "contactgegeventype": "Telefoon",
                    "waarde": "+32 2 123 45 67",
                    "isPrimair": False
                }
            ],
            "vertegenwoordigers": [
                {
                    "@id": "persoon1",
                    "vertegenwoordigerId": "v1",
                    "@type": "Persoon",
                    "voornaam": "Jan",
                    "achternaam": "Janssens",
                    "isPrimair": True,
                    "vertegenwoordigerContactgegevens": {
                        "@id": "vcontact1",
                        "@type": "Contactgegeven",
                        "e-mail": "jan@example.com"
                    }
                }
            ],
            "status": "Actief",
            "metadata": {
                "datumLaatsteAanpassing": "2024-01-15T10:30:00Z"
            },
            "etag": "test-etag-123"
        }
    ]

    # Transform the data
    result = transform_data(test_data)

    # Print results
    print("=" * 80)
    print("TRANSFORM DATA TEST RESULTS")
    print("=" * 80)
    print(f"\nInput items: {len(test_data)}")
    print(f"Output items: {len(result)}")

    if result:
        print("\n" + "-" * 80)
        print("TRANSFORMED DATA (formatted JSON):")
        print("-" * 80)
        print(json.dumps(result[0], indent=2, ensure_ascii=False))

        # Basic assertions
        assert len(result) == 1, "Expected 1 transformed item"
        assert result[0]["@type"] == "fei:Vereniging", "Expected @type to be fei:Vereniging"
        assert result[0]["vCode"] == "V0001234", "vCode mismatch"
        assert result[0]["primaireLocatie"] is not None, "Primary location should exist"
        assert len(result[0]["contactgegevens"]) == 2, "Expected 2 contact points"
        assert len(result[0]["vertegenwoordigers"]) == 1, "Expected 1 representative"

        print("\n" + "=" * 80)
        print("✓ All basic assertions passed!")
        print("=" * 80)

    return result


def test_transform_data_removed_resource():
    """Test transform_data with a removed resource and unexpected response"""

    test_data = [
        {
            "type": "RemovedResource",
            "vCode": "V9999999"
        },
        {
            "type": "UnexpectedResponse",
            "vCode": "V8888888"
        }
    ]

    result = transform_data(test_data)

    print("\n" + "=" * 80)
    print("REMOVED RESOURCE & UNEXPECTED RESPONSE TEST")
    print("=" * 80)
    print(f"Input items: {len(test_data)}")
    print(f"Output items: {len(result)}")
    print("✓ Removed resources and unexpected responses are correctly skipped")

    assert len(result) == 0, "Removed resources and unexpected responses should be skipped"

    return result


def test_transform_data_no_location():
    """Test transform_data with missing location or empty location array (should skip)"""

    test_data = [
        {
            "vCode": "V0005678",
            "naam": "Test With Empty Location Array",
            "verenigingstype": {"code": "FWB"},
            "sleutels": [{"codeerSysteem": "Vcode", "waarde": "V0005678"}],
            # Empty locaties field
            "locaties": [],
            "status": "Actief",
            "metadata": {"datumLaatsteAanpassing": "2024-01-15T10:30:00Z"}
        },
        {
            "vCode": "V0005679",
            "naam": "Test Without Location Field",
            "verenigingstype": {"code": "FWB"},
            "sleutels": [{"codeerSysteem": "Vcode", "waarde": "V0005679"}],
            # No locaties field at all
            "status": "Actief",
            "metadata": {"datumLaatsteAanpassing": "2024-01-15T10:30:00Z"}
        }
    ]

    result = transform_data(test_data)

    print("\n" + "=" * 80)
    print("NO LOCATION TEST (EMPTY ARRAY & MISSING FIELD)")
    print("=" * 80)
    print(f"Input items: {len(test_data)}")
    print(f"Output items: {len(result)}")
    print("✓ Items with empty location array are correctly skipped")
    print("✓ Items without location field are correctly skipped")

    assert len(result) == 0, "Items without location or with empty location array should be skipped"

    return result

def test_transform_data_duplicate_status():
    """Test transform_data with 'Dubbel' status (should skip)"""

    test_data = [
        {
            "vCode": "V0009101",
            "naam": "Test Dubbel Status",
            "verenigingstype": {"code": "FWB"},
            "sleutels": [{"codeerSysteem": "Vcode", "waarde": "V0009101"}],
            "locaties": [
                {
                    "@id": "loc1",
                    "locatieId": "12345",
                    "@type": "Locatie",
                    "naam": "Hoofdzetel",
                    "locatietype": "Maatschappelijke zetel volgens KBO",
                    "isPrimair": True,
                    "adres": {
                        "straatnaam": "Dubbelstraat",
                        "huisnummer": "2",
                        "postcode": "2000",
                        "gemeente": "Antwerpen"
                    },
                    "adresvoorstelling": "Dubbelstraat 2, 2000 Antwerpen",
                    "verwijstNaar": {
                        "@id": "https://data.vlaanderen.be/id/adres/654321"
                    }
                }
            ],
            "status": "Dubbel",
            "metadata": {"datumLaatsteAanpassing": "2024-01-15T10:30:00Z"}
        }
    ]

    result = transform_data(test_data)

    print("\n" + "=" * 80)
    print("DUPLICATE STATUS TEST")
    print("=" * 80)
    print(f"Input items: {len(test_data)}")
    print(f"Output items: {len(result)}")
    print("✓ Items with 'Dubbel' status are correctly skipped")

    assert len(result) == 0, "Items with 'Dubbel' status should be skipped"

    return result


def test_transform_data_postal_code_outside_region():
    """Test transform_data with postal codes outside Flanders or Brussels (should skip)"""

    test_data = [
        {
            "vCode": "V0001112",
            "naam": "Test Outside Region",
            "verenigingstype": {"code": "FWB"},
            "sleutels": [{"codeerSysteem": "Vcode", "waarde": "V0001112"}],
            "locaties": [
                {
                    "@id": "loc1",
                    "locatieId": "12345",
                    "@type": "Locatie",
                    "naam": "Hoofdzetel",
                    "locatietype": "Maatschappelijke zetel volgens KBO",
                    "isPrimair": True,
                    "adres": {
                        "straatnaam": "Rue de Test",
                        "huisnummer": "10",
                        "postcode": "4000",  # Liège - Wallonia, not Flanders or Brussels
                        "gemeente": "Liège"
                    },
                    "adresvoorstelling": "Rue de Test 10, 4000 Liège"
                }
            ],
            "status": "Actief",
            "metadata": {"datumLaatsteAanpassing": "2024-01-15T10:30:00Z"}
        }
    ]

    result = transform_data(test_data)

    print("\n" + "=" * 80)
    print("POSTAL CODE OUTSIDE REGION TEST")
    print("=" * 80)
    print(f"Input items: {len(test_data)}")
    print(f"Output items: {len(result)}")
    print("✓ Items with all postal codes outside Flanders or Brussels are correctly skipped")

    assert len(result) == 0, "Items with postal codes outside Flanders or Brussels should be skipped"

    return result

def test_transform_data_valid_flanders_postal_code():
    """Test transform_data with valid Flanders postal code (should process)"""

    test_data = [
        {
            "vCode": "V0001516",
            "naam": "Test Flanders Valid",
            "verenigingstype": {"code": "FWB"},
            "sleutels": [{"codeerSysteem": "Vcode", "waarde": "V0001516"}],
            "locaties": [
                {
                    "@id": "loc1",
                    "locatieId": "12345",
                    "@type": "Locatie",
                    "naam": "Hoofdzetel",
                    "locatietype": "Maatschappelijke zetel volgens KBO",
                    "isPrimair": True,
                    "adres": {
                        "straatnaam": "Gentstraat",
                        "huisnummer": "5",
                        "postcode": "9000",  # Gent - Flanders
                        "gemeente": "Gent"
                    },
                    "adresvoorstelling": "Gentstraat 5, 9000 Gent"
                }
            ],
            "status": "Actief",
            "metadata": {"datumLaatsteAanpassing": "2024-01-15T10:30:00Z"}
        }
    ]

    result = transform_data(test_data)

    print("\n" + "=" * 80)
    print("VALID FLANDERS POSTAL CODE TEST")
    print("=" * 80)
    print(f"Input items: {len(test_data)}")
    print(f"Output items: {len(result)}")
    print("✓ Items with valid Flanders postal codes are correctly processed")

    assert len(result) == 1, "Items with valid Flanders postal codes should be processed"
    assert result[0]["vCode"] == "V0001516", "vCode mismatch"

    return result


def test_transform_data_from_file():
    """Test transform_data with all data from data.json.gz"""
    import gzip
    import os
    import time

    # Load data from gzip file
    test_file_path = os.path.join(os.path.dirname(__file__), "data.json.gz")

    print("\n" + "=" * 80)
    print("FULL DATA.JSON.GZ TRANSFORMATION TEST")
    print("=" * 80)
    print(f"Loading data from: {test_file_path}")

    start_load = time.time()
    with gzip.open(test_file_path, 'rt', encoding='utf-8') as f:
        test_data = json.load(f)
    load_time = time.time() - start_load

    print(f"✓ Loaded {len(test_data)} items in {load_time:.2f} seconds")

    # Transform the data
    print("\nTransforming data...")
    start_transform = time.time()
    result = transform_data(test_data)
    transform_time = time.time() - start_transform

    print(f"✓ Transformed {len(result)} items in {transform_time:.2f} seconds")
    print(f"✓ Skipped {len(test_data) - len(result)} items")

    # Basic validation
    if result:
        print("\n" + "-" * 80)
        print("SAMPLE TRANSFORMED DATA (first item):")
        print("-" * 80)
        print(json.dumps(result[0], indent=2, ensure_ascii=False)[:2000] + "...")

        # Validate structure of transformed items
        all_have_type = all(item.get("@type") == "fei:Vereniging" for item in result)
        all_have_vcode = all("vCode" in item for item in result)
        all_have_location = all("primaireLocatie" in item for item in result)

        print("\n" + "-" * 80)
        print("VALIDATION CHECKS:")
        print("-" * 80)
        print(f"✓ All items have @type='fei:Vereniging': {all_have_type}")
        print(f"✓ All items have vCode: {all_have_vcode}")
        print(f"✓ All items have primaireLocatie: {all_have_location}")

        assert all_have_type, "All transformed items should have @type='fei:Vereniging'"
        assert all_have_vcode, "All transformed items should have vCode"
        assert all_have_location, "All transformed items should have primaireLocatie"

    print("\n" + "=" * 80)
    print(f"✓ Full data transformation test completed successfully!")
    print(f"  Total time: {load_time + transform_time:.2f} seconds")
    print("=" * 80)

    return result
if __name__ == "__main__":
    print("\n" + "╔" + "=" * 78 + "╗")
    print("║" + " " * 20 + "RUNNING TRANSFORM DATA TESTS" + " " * 30 + "║")
    print("╚" + "=" * 78 + "╝")

    try:
        # Run tests
        test_transform_data_basic()
        test_transform_data_removed_resource()
        test_transform_data_no_location()
        test_transform_data_duplicate_status()
        test_transform_data_postal_code_outside_region()
        test_transform_data_valid_flanders_postal_code()
        # test_transform_data_from_file()

        print("\n" + "╔" + "=" * 78 + "╗")
        print("║" + " " * 25 + "ALL TESTS PASSED ✓" + " " * 35 + "║")
        print("╚" + "=" * 78 + "╝\n")

    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}\n")
        raise
