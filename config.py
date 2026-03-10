OFFICE_TOWNS = [
    "Godalming", "Guildford", "Farnham", "Woking", "Haslemere",
    "Cranleigh", "Milford", "Shalford", "Compton", "Bramley",
    "Hindhead", "Elstead", "Witley", "Chiddingfold", "Dunsfold",
    "Alfold", "Busbridge", "Hascombe", "Shackleford", "Puttenham",
    "Thursley", "Farncombe", "Eashing", "Hurtmore", "Peperharow",
    "Hambledon",
]

WELLNESS_TOWNS = [
    "Godalming", "Guildford", "Farnham", "Woking", "Haslemere",
    "Cranleigh", "Milford", "Shalford", "Compton", "Bramley", "Hindhead",
]

DEFAULT_TOWNS = ["Guildford", "Godalming", "Farnham", "Woking"]

OFFICE_OUTPUT_FILE = "office_leads.csv"
UNIT8_OUTPUT_FILE = "unit8_leads_enriched.csv"

OFFICE_GU_POSTCODES = [
    "GU1", "GU2", "GU3", "GU4", "GU5", "GU6", "GU7", "GU8", "GU9",
    "GU10", "GU11", "GU12", "GU27", "GU28", "GU29", "GU30",
]

OFFICE_SIC_CODES = {
    "management_consultancy": ["70100", "70210", "70220", "70229"],
    "software_it": ["62011", "62012", "62020", "62090"],
    "legal": ["69101", "69102", "69109"],
    "accounting": ["69201", "69202", "69203"],
    "recruitment": ["78100", "78200", "78300"],
    "marketing_pr": ["73110", "73120", "73200"],
    "engineering": ["71121", "71122", "71129"],
    "environmental": ["71112", "74909"],
    "financial_services": ["64300", "64991", "66190"],
    "architecture": ["71111"],
    "property_management": ["68320"],
    "training_coaching": ["85590", "85600"],
}

OFFICE_SIC_CODES_FLAT = []
for codes in OFFICE_SIC_CODES.values():
    OFFICE_SIC_CODES_FLAT.extend(codes)

SIC_CODE_TO_SECTOR = {}
for sector, codes in OFFICE_SIC_CODES.items():
    label = sector.replace("_", " ").title()
    for code in codes:
        SIC_CODE_TO_SECTOR[code] = label
