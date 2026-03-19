"""
Extract LLM-inferred columns from billionaire_survey_augmented_fixed.xlsx.

These fields were populated by Claude Opus 4 via fill_row.py, using the model's
training knowledge (no web search). The output CSV is named to make the
provenance clear: this is LLM inference, not ground-truth from an API.
"""

import re

import openpyxl
import pandas as pd
import os

XLSX = os.path.join(os.path.dirname(__file__), "../../../billionaire_survey_augmented_fixed.xlsx")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "../intermediate/claude_opus_4")

# Column number → clean snake_case name
# Only columns primarily filled by LLM inference (not Forbes API or SEC EDGAR)
COLUMNS = {
    # Identity & Demographics
    7: "age_became_billionaire",
    10: "number_of_marriages",
    14: "ethnicity",
    15: "first_gen_immigrant",
    16: "parent_immigration_status",

    # Family Background
    17: "father_occupation",
    18: "mother_occupation",
    19: "parent_education_level",
    20: "family_socioeconomic_class",
    21: "family_owned_business",
    22: "family_business_type",
    23: "estimated_inheritance_m",

    # Education
    30: "age_education_ended",

    # Pre-founding Career
    32: "employer_1",
    33: "role_at_employer_1",
    34: "years_at_employer_1",
    35: "employer_2",
    36: "role_at_employer_2",
    37: "years_at_employer_2",
    38: "employer_3",
    39: "role_at_employer_3",
    40: "years_at_employer_3",
    41: "total_pre_founding_career_years",
    42: "primary_domain_expertise",
    43: "skill_profile",

    # Founding & Company
    46: "year_founded",
    47: "age_at_founding",
    48: "solo_or_co_founded",
    49: "number_of_co_founders",
    50: "co_founder_skill_complement",
    53: "wealth_concentration_ratio_pct",
    56: "number_of_companies_founded",
    57: "known_failed_ventures",
    58: "years_founding_to_1b",
    59: "initial_funding_type",
    60: "first_institutional_investor",
    72: "revenue_model_type",
    73: "moat_type",
    74: "market_position",

    # Lifestyle & Consumption
    82: "private_jet",
    84: "yacht",
    86: "known_art_collector",
    88: "consumption_index",

    # Philanthropy
    89: "giving_pledge_signatory",
    90: "foundation_name",
    93: "primary_philanthropic_causes",
    95: "estimated_lifetime_giving_m",
    96: "giving_pct_of_net_worth",

    # Political
    98: "primary_party_affiliation",
    99: "notable_political_positions",
    101: "government_roles_held",

    # Family & Succession
    102: "children_in_business",
    104: "family_office",
    105: "family_office_name",

    # Legal
    109: "major_lawsuits",
    111: "pre_wealth_bankruptcy",

    # Public Profile
    114: "media_visibility",
    116: "books_authored",
    117: "social_media_activity",
}

GAPMINDER_SLUG_COL = 128  # 1-based


def split_parens(val):
    """Split 'Base text (detail)' into ('Base text', 'detail'). Handles nested parens."""
    if pd.isna(val):
        return pd.NA, pd.NA
    val = str(val).strip()
    m = re.match(r'^(.*?)\s*\((.+)\)\s*$', val)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return val, pd.NA


"""
Ethnicity normalization: maps ~144 free-text values to ~50 canonical entity IDs.
"""

# canonical entity ID → (display name, broad group)
ETHNICITY_ENTITIES = {
    "white_american": ("White American", "European"),
    "american": ("American (unspecified)", "Unspecified"),
    "jewish_american": ("Jewish American", "Jewish"),
    "italian_american": ("Italian American", "European"),
    "indian_american": ("Indian American", "South Asian"),
    "african_american": ("African American", "African American"),
    "chinese_american": ("Chinese American", "East Asian"),
    "taiwanese_american": ("Taiwanese American", "East Asian"),
    "greek_american": ("Greek American", "European"),
    "israeli_american": ("Israeli American", "Jewish"),
    "german_american": ("German American", "European"),
    "canadian_american": ("Canadian American", "European"),
    "irish_american": ("Irish American", "European"),
    "dutch_american": ("Dutch American", "European"),
    "ukrainian_american": ("Ukrainian American", "European"),
    "hungarian_american": ("Hungarian American", "European"),
    "french_american": ("French American", "European"),
    "turkish_american": ("Turkish American", "Middle Eastern"),
    "iranian_american": ("Iranian American", "Middle Eastern"),
    "brazilian_american": ("Brazilian American", "Hispanic/Latino"),
    "norwegian_american": ("Norwegian American", "European"),
    "mexican_american": ("Mexican American", "Hispanic/Latino"),
    "colombian_american": ("Colombian American", "Hispanic/Latino"),
    "korean_american": ("Korean American", "East Asian"),
    "south_african_american": ("South African American", "European"),
    "romanian_american": ("Romanian American", "European"),
    "australian_american": ("Australian American", "European"),
    "lebanese_american": ("Lebanese American", "Middle Eastern"),
    "puerto_rican_american": ("Puerto Rican American", "Hispanic/Latino"),
    "scottish_american": ("Scottish American", "European"),
    "iraqi_israeli_american": ("Iraqi/Israeli American", "Jewish"),
    "hispanic_american": ("Hispanic American", "Hispanic/Latino"),
    "russian_american": ("Russian American", "European"),
    "cuban_american": ("Cuban American", "Hispanic/Latino"),
    "asian_american": ("Asian American", "East Asian"),
    "indonesian_american": ("Indonesian American", "Southeast Asian"),
    "moroccan_american": ("Moroccan American", "Middle Eastern"),
    "haitian_american": ("Haitian American", "African American"),
    "panamanian_american": ("Panamanian American", "Hispanic/Latino"),
    "pakistani_american": ("Pakistani American", "South Asian"),
    "bulgarian_american": ("Bulgarian American", "European"),
    "austrian_american": ("Austrian American", "European"),
    "nigerian_american": ("Nigerian American", "African American"),
    "venezuelan_american": ("Venezuelan American", "Hispanic/Latino"),
    "filipino_american": ("Filipino American", "Southeast Asian"),
    "japanese_american": ("Japanese American", "East Asian"),
    "polish_american": ("Polish American", "European"),
    "armenian_american": ("Armenian American", "Middle Eastern"),
    "czech_american": ("Czech American", "European"),
    "welsh_american": ("Welsh American", "European"),
    "belarusian_american": ("Belarusian American", "European"),
    "barbadian_american": ("Barbadian American", "African American"),
    "palestinian_american": ("Palestinian American", "Middle Eastern"),
    "white_south_african": ("White South African", "European"),
    "italian_greek_american": ("Italian/Greek American", "European"),
    "hispanic_irish_american": ("Hispanic and Irish American", "Mixed/Other"),
    "argentine_cuban_american": ("Argentine/Cuban American", "Hispanic/Latino"),
    "armenian_lebanese_american": ("Armenian/Lebanese American", "Middle Eastern"),
    "lebanese_egyptian_american": ("Lebanese/Egyptian American", "Middle Eastern"),
    "french_iranian_american": ("French/Iranian American", "Mixed/Other"),
    "lebanese_israeli_american": ("Lebanese/Israeli American", "Jewish"),
    "colombian_latin_american": ("Colombian/Latin American", "Hispanic/Latino"),
    "malaysian_chinese_american": ("Malaysian Chinese American", "East Asian"),
    "chinese_taiwanese_american": ("Chinese/Taiwanese American", "East Asian"),
    "south_african_chinese": ("South African-born Chinese", "East Asian"),
    "indonesian_chinese": ("Indonesian Chinese", "East Asian"),
    "singaporean_chinese": ("Singaporean Chinese", "East Asian"),
    "filipino_mixed_american": ("Filipino/Mixed American", "Mixed/Other"),
    "biracial": ("Biracial", "Mixed/Other"),
    "multiracial": ("Multiracial", "Mixed/Other"),
    "iranian_swedish": ("Iranian/Swedish", "Mixed/Other"),
    "american_swiss": ("American/Swiss", "European"),
    "south_asian": ("South Asian", "South Asian"),
    "british_american": ("British American", "European"),
    "spanish_american": ("Spanish American", "European"),
    "bolivian_american": ("Bolivian American", "Hispanic/Latino"),
    "kenyan_american": ("Kenyan American", "African American"),
    "swedish_american": ("Swedish American", "European"),
    "swiss_american": ("Swiss American", "European"),
    "turkmen_american": ("Turkmen American", "Middle Eastern"),
}

# raw value (case-insensitive) → canonical entity ID
ETHNICITY_MAP = {
    # White American variants
    "white american": "white_american",
    "white / american": "white_american",
    "white/american": "white_american",
    "white/caucasian": "white_american",
    "white": "white_american",
    "caucasian": "white_american",
    "caucasian/white": "white_american",
    "american / white": "white_american",
    "white/european american": "white_american",
    "caucasian/american": "white_american",
    "white american/australian": "white_american",
    # American (unspecified)
    "american": "american",
    # Jewish variants
    "jewish american": "jewish_american",
    "jewish": "jewish_american",
    "jewish-american": "jewish_american",
    "jewish / american": "jewish_american",
    "jewish/american": "jewish_american",
    "ashkenazi jewish": "jewish_american",
    "jewish / eastern european immigrant descent": "jewish_american",
    "jewish / polish-american": "jewish_american",
    "jewish / canadian": "jewish_american",
    # Italian
    "italian-american": "italian_american",
    "italy": "italian_american",
    "sicilian-american": "italian_american",
    # Indian
    "indian-american": "indian_american",
    "india": "indian_american",
    "indian": "indian_american",
    "indian american": "indian_american",
    "south asian / indian-american": "indian_american",
    # African American
    "african american": "african_american",
    # Chinese
    "chinese-american": "chinese_american",
    "chinese / asian": "chinese_american",
    "chinese": "chinese_american",
    "china": "chinese_american",
    "hong kong-american": "chinese_american",
    # Taiwanese
    "taiwanese-american": "taiwanese_american",
    "taiwan": "taiwanese_american",
    "taiwanese american": "taiwanese_american",
    # Greek
    "greek-american": "greek_american",
    "greece": "greek_american",
    # Israeli
    "israel": "israeli_american",
    "israeli-american": "israeli_american",
    "israeli": "israeli_american",
    # German
    "german-american": "german_american",
    "germany": "german_american",
    "german": "german_american",
    # Canadian
    "canadian-american": "canadian_american",
    "canada": "canadian_american",
    "canadian": "canadian_american",
    # Irish
    "irish-american": "irish_american",
    # Dutch
    "dutch-american": "dutch_american",
    # Ukrainian
    "ukrainian-american": "ukrainian_american",
    "ukraine": "ukrainian_american",
    # Hungarian
    "hungarian-american": "hungarian_american",
    "hungarian american": "hungarian_american",
    # French
    "french": "french_american",
    "france": "french_american",
    "french-american": "french_american",
    # Turkish
    "turkish-american": "turkish_american",
    # Iranian
    "iranian-american": "iranian_american",
    "iran": "iranian_american",
    "iranian": "iranian_american",
    # Brazilian
    "brazilian-american": "brazilian_american",
    # Norwegian
    "norwegian-american": "norwegian_american",
    # Mexican
    "mexican-american": "mexican_american",
    "mexico": "mexican_american",
    "hispanic / mexican-american": "mexican_american",
    # Colombian
    "colombian-american": "colombian_american",
    "colombian": "colombian_american",
    "colombian/latin american": "colombian_latin_american",
    # Korean
    "korean-american": "korean_american",
    "south korea": "korean_american",
    "korean american": "korean_american",
    # South African
    "south african-american": "south_african_american",
    "south africa": "south_african_american",
    "white south african": "white_south_african",
    # Romanian
    "romanian": "romanian_american",
    "romanian-american": "romanian_american",
    # Australian
    "australian": "australian_american",
    "australia": "australian_american",
    "australian-born": "australian_american",
    # Lebanese
    "lebanese-american": "lebanese_american",
    "lebanese": "lebanese_american",
    # Puerto Rican
    "puerto rican-american": "puerto_rican_american",
    "puerto rico": "puerto_rican_american",
    # Scottish
    "scottish": "scottish_american",
    "scottish-american": "scottish_american",
    # Iraqi/Israeli
    "iraqi/israeli heritage": "iraqi_israeli_american",
    "iraq": "iraqi_israeli_american",
    # Hispanic
    "hispanic american": "hispanic_american",
    "hispanic": "hispanic_american",
    "hispanic/latino": "hispanic_american",
    # Russian
    "russian-american": "russian_american",
    "russia": "russian_american",
    "russian": "russian_american",
    # Cuban
    "cuban american": "cuban_american",
    # Asian
    "asian american": "asian_american",
    "asian": "asian_american",
    # Indonesian
    "indonesian": "indonesian_american",
    "indonesia": "indonesian_american",
    # Moroccan
    "moroccan-american": "moroccan_american",
    # Haitian
    "haiti": "haitian_american",
    # Panamanian
    "panama": "panamanian_american",
    # Pakistani
    "pakistani-american": "pakistani_american",
    "pakistani": "pakistani_american",
    # Bulgarian
    "bulgarian-american": "bulgarian_american",
    # Austrian
    "austrian-american": "austrian_american",
    # Nigerian
    "nigerian-american": "nigerian_american",
    "nigeria": "nigerian_american",
    # Venezuelan
    "venezuela": "venezuelan_american",
    # Filipino
    "filipino-american": "filipino_american",
    "filipino-american / mixed": "filipino_mixed_american",
    # Japanese
    "japanese-american": "japanese_american",
    # Polish
    "polish-american": "polish_american",
    # Armenian
    "armenian-american": "armenian_american",
    # Czech
    "czech-american": "czech_american",
    # Welsh
    "welsh-american": "welsh_american",
    # Belarusian
    "belarusian-american": "belarusian_american",
    # Barbadian
    "barbadian-american": "barbadian_american",
    # Palestinian
    "palestinian-american": "palestinian_american",
    # Compound/Mixed
    "italian/greek-american": "italian_greek_american",
    "hispanic and irish-american": "hispanic_irish_american",
    "argentine-cuban-american": "argentine_cuban_american",
    "armenian-lebanese-american": "armenian_lebanese_american",
    "lebanese-egyptian-american": "lebanese_egyptian_american",
    "french-iranian-american": "french_iranian_american",
    "lebanese-israeli-american": "lebanese_israeli_american",
    "malaysian-chinese american": "malaysian_chinese_american",
    "chinese-taiwanese-american": "chinese_taiwanese_american",
    "south african-born chinese": "south_african_chinese",
    "indonesian-chinese": "indonesian_chinese",
    "singaporean/chinese": "singaporean_chinese",
    # Mixed/Other
    "biracial": "biracial",
    "multiracial": "multiracial",
    "iranian/swedish": "iranian_swedish",
    "american/swiss": "american_swiss",
    # South Asian
    "south asian": "south_asian",
    # Country-name entries
    "united kingdom": "british_american",
    "spain": "spanish_american",
    "bolivia": "bolivian_american",
    "kenya": "kenyan_american",
    "sweden": "swedish_american",
    "swiss": "swiss_american",
    "turkmenistan": "turkmen_american",
}

# Multi-word ethnicity tokens that should NOT be split on underscore
MULTI_WORD_ETHNICITIES = {
    'african_american', 'south_african', 'south_asian',
    'puerto_rican', 'latin_american',
}

# Base ethnicity token → display name (for entity file)
BASE_ETHNICITY_NAMES = {
    "african_american": "African American",
    "american": "American",
    "argentine": "Argentine",
    "armenian": "Armenian",
    "asian": "Asian",
    "australian": "Australian",
    "austrian": "Austrian",
    "barbadian": "Barbadian",
    "belarusian": "Belarusian",
    "biracial": "Biracial",
    "bolivian": "Bolivian",
    "brazilian": "Brazilian",
    "british": "British",
    "bulgarian": "Bulgarian",
    "canadian": "Canadian",
    "chinese": "Chinese",
    "colombian": "Colombian",
    "cuban": "Cuban",
    "czech": "Czech",
    "dutch": "Dutch",
    "egyptian": "Egyptian",
    "filipino": "Filipino",
    "french": "French",
    "german": "German",
    "greek": "Greek",
    "haitian": "Haitian",
    "hispanic": "Hispanic",
    "hungarian": "Hungarian",
    "indian": "Indian",
    "indonesian": "Indonesian",
    "iranian": "Iranian",
    "iraqi": "Iraqi",
    "irish": "Irish",
    "israeli": "Israeli",
    "italian": "Italian",
    "japanese": "Japanese",
    "jewish": "Jewish",
    "kenyan": "Kenyan",
    "korean": "Korean",
    "latin_american": "Latin American",
    "lebanese": "Lebanese",
    "malaysian": "Malaysian",
    "mexican": "Mexican",
    "mixed": "Mixed",
    "moroccan": "Moroccan",
    "multiracial": "Multiracial",
    "nigerian": "Nigerian",
    "norwegian": "Norwegian",
    "pakistani": "Pakistani",
    "palestinian": "Palestinian",
    "panamanian": "Panamanian",
    "polish": "Polish",
    "puerto_rican": "Puerto Rican",
    "romanian": "Romanian",
    "russian": "Russian",
    "scottish": "Scottish",
    "singaporean": "Singaporean",
    "south_african": "South African",
    "south_asian": "South Asian",
    "spanish": "Spanish",
    "swedish": "Swedish",
    "swiss": "Swiss",
    "taiwanese": "Taiwanese",
    "turkish": "Turkish",
    "turkmen": "Turkmen",
    "ukrainian": "Ukrainian",
    "venezuelan": "Venezuelan",
    "welsh": "Welsh",
    "white": "White",
}


# Enum ID mappings: display name → snake_case entity ID
SOCIOECONOMIC_IDS = {"Poor": "poor", "Working": "working", "Lower-Middle": "lower_middle", "Middle": "middle", "Upper-Middle": "upper_middle", "Wealthy": "wealthy"}
CONSUMPTION_IDS = {"Low": "low", "Medium": "medium", "High": "high"}
MARKET_POSITION_IDS = {"Fragmented": "fragmented", "Winner-Take-Most": "winner_take_most", "Winner-Take-All": "winner_take_all"}
MEDIA_VISIBILITY_IDS = {"Low": "low", "Medium": "medium", "High": "high"}
PARENT_EDUCATION_IDS = {"No High School": "no_high_school", "High School": "high_school", "College": "college", "Graduate/Professional": "graduate_professional", "Unknown": "unknown"}
PARTY_IDS = {"Democrat": "democrat", "Republican": "republican", "Independent": "independent", "Libertarian": "libertarian"}
SOCIAL_MEDIA_IDS = {"Low": "low", "Medium": "medium", "High": "high"}


def decompose_ethnicity(val):
    """Split compound ethnicity ID into comma-separated base tokens."""
    if pd.isna(val):
        return pd.NA
    tokens = str(val).split('_')
    parts = []
    i = 0
    while i < len(tokens):
        if i + 1 < len(tokens):
            bigram = tokens[i] + '_' + tokens[i + 1]
            if bigram in MULTI_WORD_ETHNICITIES:
                parts.append(bigram)
                i += 2
                continue
        parts.append(tokens[i])
        i += 1
    return ",".join(sorted(set(parts)))


def normalize(df):
    """Normalize columns: split compound fields, standardize categories."""

    # 1. primary_party_affiliation → party + party_detail
    if "primary_party_affiliation" in df.columns:
        party_map = {
            "republican": "Republican",
            "democrat": "Democrat",
            "democratic": "Democrat",
            "democrat-leaning": "Democrat",
            "republican-leaning": "Republican",
            "independent": "Independent",
            "libertarian": "Libertarian",
            "libertarian-leaning": "Libertarian",
            "conservative/libertarian": "Republican",
            "nonpartisan": "Independent",
            "none": "Independent",
        }

        def classify_party(val):
            if pd.isna(val):
                return pd.NA, pd.NA
            val = str(val).strip()
            # Check for parenthetical
            base, detail = split_parens(val)
            # Try exact map on full value first, then base
            key = val.lower().strip()
            if key in party_map:
                return party_map[key], detail
            key = base.lower().strip()
            if key in party_map:
                return party_map[key], detail if pd.notna(detail) else pd.NA
            # Check if starts with a known party
            for prefix, party in [("republican", "Republican"), ("democrat", "Democrat"),
                                  ("independent", "Independent"), ("libertarian", "Libertarian")]:
                if key.startswith(prefix):
                    # Everything after the base word is detail
                    remainder = val[len(prefix):].strip(" /;,-()")
                    return party, remainder if remainder else pd.NA
            # Mixed/bipartisan
            if "bipartisan" in key or "mixed" in key or "both" in key:
                return "Independent", val
            return pd.NA, val  # Can't classify — keep raw as detail

        results = df["primary_party_affiliation"].apply(classify_party)
        df["party"] = results.apply(lambda x: x[0])
        df["party_detail"] = results.apply(lambda x: x[1])
        df["party"] = df["party"].map(PARTY_IDS)
        df = df.drop(columns=["primary_party_affiliation"])

    # 2. market_position → normalize to 3 categories + detail
    if "market_position" in df.columns:
        def classify_market(val):
            if pd.isna(val):
                return pd.NA, pd.NA
            val = str(val).strip()
            base, detail = split_parens(val)
            low = base.lower().replace("-", "").replace(" ", "")
            if "winnertakeall" in low:
                return "Winner-Take-All", detail
            if "winnertakemost" in low or "most" in low:
                return "Winner-Take-Most", detail
            if "fragmented" in low:
                return "Fragmented", detail
            # Check with parens included
            low_full = val.lower().replace("-", "").replace(" ", "")
            if "winnertakeall" in low_full:
                return "Winner-Take-All", detail
            if "winnertakemost" in low_full:
                return "Winner-Take-Most", detail
            return "Fragmented", detail

        results = df["market_position"].apply(classify_market)
        df["market_position"] = results.apply(lambda x: x[0])
        df["market_position_detail"] = results.apply(lambda x: x[1])
        df["market_position"] = df["market_position"].map(MARKET_POSITION_IDS)

    # 3. family_socioeconomic_class → normalize to 5 categories
    if "family_socioeconomic_class" in df.columns:
        def classify_class(val):
            if pd.isna(val):
                return pd.NA
            low = str(val).lower().strip()
            low = re.sub(r'\s*\(.*\)', '', low).strip()
            if any(k in low for k in ("wealthy", "upper class", "upper-class", "affluent",
                                       "rich", "elite", "ultra", "billionaire")):
                return "Wealthy"
            if "upper" in low and "middle" in low:
                return "Upper-Middle"
            if ("middle" in low or "moderate" in low) and ("lower" in low or "working" in low):
                return "Lower-Middle"
            if "middle" in low or "moderate" in low:
                return "Middle"
            if any(k in low for k in ("poor", "poverty", "impoverished", "destitute",
                                       "low-income", "low income", "projects", "orphan")):
                return "Poor"
            if "working" in low or "blue" in low or "lower" in low or "low" in low:
                return "Working"
            if "business" in low:
                return "Middle"
            if "upper" in low:
                return "Wealthy"
            return val  # keep as-is if can't classify

        df["family_socioeconomic_class"] = df["family_socioeconomic_class"].apply(classify_class)
        df["family_socioeconomic_class"] = df["family_socioeconomic_class"].map(SOCIOECONOMIC_IDS)

    # 4. media_visibility → normalize Med→Medium
    if "media_visibility" in df.columns:
        df["media_visibility"] = df["media_visibility"].replace({"Med": "Medium"})
        df["media_visibility"] = df["media_visibility"].map(MEDIA_VISIBILITY_IDS)

    # 5. solo_or_co_founded → co_founded (Y/N) + founding_detail
    if "solo_or_co_founded" in df.columns:
        def classify_founding(val):
            if pd.isna(val):
                return pd.NA, pd.NA
            val = str(val).strip()
            base, detail = split_parens(val)
            low = base.lower()
            if low.startswith("co"):
                return "Y", detail
            if low == "solo":
                return "N", detail
            if "inherit" in low or "family" in low:
                return pd.NA, val  # not applicable
            return pd.NA, val

        results = df["solo_or_co_founded"].apply(classify_founding)
        df["co_founded"] = results.apply(lambda x: x[0])
        df["founding_detail"] = results.apply(lambda x: x[1])
        df = df.drop(columns=["solo_or_co_founded"])

    # 6. ethnicity → split parens, then map to canonical entity IDs
    if "ethnicity" in df.columns:
        results = df["ethnicity"].apply(split_parens)
        df["ethnicity"] = results.apply(lambda x: x[0])
        df["heritage_detail"] = results.apply(lambda x: x[1])

        # Map free-text to canonical entity IDs
        def map_ethnicity(val):
            if pd.isna(val):
                return pd.NA
            key = str(val).strip().lower()
            if key in ETHNICITY_MAP:
                return ETHNICITY_MAP[key]
            # Fallback: slugify
            slug = re.sub(r'[^a-z0-9]+', '_', key).strip('_')
            print(f"  WARNING: unmapped ethnicity '{val}' → '{slug}'")
            return slug

        df["ethnicity"] = df["ethnicity"].apply(map_ethnicity)

        # Decompose compound ethnicity IDs into comma-separated base tokens
        df["ethnicity"] = df["ethnicity"].apply(decompose_ethnicity)

    # 7. parent_immigration_status → parent_immigrant (Y/N) + parent_immigration_detail
    if "parent_immigration_status" in df.columns:
        def classify_parent_immig(val):
            if pd.isna(val):
                return pd.NA, pd.NA
            val = str(val).strip()
            base, detail = split_parens(val)
            low = base.lower()
            if any(k in low for k in ["immigrant", "immigrat", "refugee", "fled", "emigrat"]):
                if any(k in low for k in ["not ", "non-", "non ", "native", "us-born",
                                           "u.s.-born", "both us", "american-born"]):
                    return "N", detail
                return "Y", detail
            if any(k in low for k in ["native", "us-born", "u.s.-born", "american-born",
                                       "both us", "born in us", "born in the us",
                                       "domestic"]):
                return "N", detail
            if any(k in low for k in ["first-gen", "first gen"]):
                return "Y", detail
            return pd.NA, val

        results = df["parent_immigration_status"].apply(classify_parent_immig)
        df["parent_immigrant"] = results.apply(lambda x: x[0])
        df["parent_immigration_detail"] = results.apply(lambda x: x[1])
        df = df.drop(columns=["parent_immigration_status"])

    # 8. social_media_activity: "Med" → "Medium"
    if "social_media_activity" in df.columns:
        df["social_media_activity"] = df["social_media_activity"].replace({"Med": "Medium"})
        df["social_media_activity"] = df["social_media_activity"].map(SOCIAL_MEDIA_IDS)

    # 9. books_authored: fix text entry → count
    if "books_authored" in df.columns:
        df["books_authored"] = df["books_authored"].replace(
            {"Breaking History: A White House Memoir (2022)": "1"}
        )

    # 10. known_failed_ventures: clear "none" variants → NA
    if "known_failed_ventures" in df.columns:
        none_vals = {
            "0", "None mentioned", "N", "None known", "None identified",
            "None explicitly stated", "None widely known", "None noted",
        }
        df["known_failed_ventures"] = df["known_failed_ventures"].apply(
            lambda v: pd.NA if (pd.notna(v) and (
                str(v).strip() in none_vals or re.match(r'^none\b', str(v).strip(), re.IGNORECASE)
            )) else v
        )

    # 11. government_roles_held: clear "none" variants → NA
    if "government_roles_held" in df.columns:
        gov_none_vals = {
            "None mentioned", "None listed", "None stated", "0",
            "None noted", "N", "None (no government roles held)",
        }
        df["government_roles_held"] = df["government_roles_held"].apply(
            lambda v: pd.NA if (pd.notna(v) and (
                str(v).strip() in gov_none_vals
                or re.match(r'^none\b|^n/a\b|^0$', str(v).strip(), re.IGNORECASE)
            )) else v
        )

    # 12. first_institutional_investor: text standardization
    if "first_institutional_investor" in df.columns:
        investor_name_map = {
            "accel": "Accel",
            "a16z": "Andreessen Horowitz",
        }
        investor_none_vals = {
            "None (bootstrapped)", "None mentioned", "None (no VC raised)",
            "N/A (private)", "N", "None — self-funded", "None (self-funded)",
        }

        def normalize_investor(val):
            if pd.isna(val):
                return pd.NA
            s = str(val).strip()
            if s in investor_none_vals or s.startswith("None (self-funded"):
                return pd.NA
            low = s.lower()
            if low in investor_name_map:
                return investor_name_map[low]
            return s

        df["first_institutional_investor"] = df["first_institutional_investor"].apply(normalize_investor)

    # 13. parent_education_level: normalize to 5 levels
    if "parent_education_level" in df.columns:
        def classify_parent_edu(val):
            if pd.isna(val):
                return pd.NA
            low = str(val).lower().strip()
            if any(k in low for k in ("unknown", "not stated", "not documented",
                                       "not known", "not recorded", "not available")):
                return "Unknown"
            if any(k in low for k in ("no school", "elementary", "no formal",
                                       "illiterate", "little formal")):
                return "No High School"
            if any(k in low for k in ("phd", "doctorate", "doctoral", " md", "m.d.",
                                       " jd", "j.d.", "mba", "m.b.a.", "law degree",
                                       "law school", "medical school", "medical degree",
                                       "master", "graduate degree", "graduate school",
                                       "professor", "advanced degree", "postgraduate")):
                return "Graduate/Professional"
            if any(k in low for k in ("high school", "no college", "dropout",
                                       "didn't attend", "did not attend college",
                                       "ged", "secondary school", "grammar school",
                                       "trade school", "vocational")):
                return "High School"
            if any(k in low for k in ("college", "university", "bachelor", "undergraduate",
                                       "degree", "b.a.", "b.s.", "educated")):
                return "College"
            return "Unknown"

        df["parent_education_level"] = df["parent_education_level"].apply(classify_parent_edu)
        df["parent_education_level"] = df["parent_education_level"].map(PARENT_EDUCATION_IDS)

    # 14. consumption_index → entity IDs
    if "consumption_index" in df.columns:
        df["consumption_index"] = df["consumption_index"].map(CONSUMPTION_IDS)

    # 15. initial_funding_type → entity domain
    if "initial_funding_type" in df.columns:
        funding_keywords = [
            ("self_funded", ["self-funded", "bootstrapped", "personal savings",
                             "personal capital", "own money", "own savings", "self funded"]),
            ("venture_capital", ["venture capital", "vc fund", "vc round"]),
            ("angel", ["angel"]),
            ("private_equity", ["private equity", "buyout", "lbo"]),
            ("inheritance", ["inheritance", "inherited", "family fortune", "trust fund"]),
            ("family_capital", ["family capital", "family money", "family loan",
                                "family investment", "parents"]),
            ("debt_financing", ["bank loan", "debt", "sba loan", "mortgage", "credit"]),
            ("government", ["government", "grant", "sbir", "darpa", "military contract"]),
            ("accelerator", ["y combinator", "techstars", "accelerator", "incubator"]),
            ("strategic", ["strategic", "corporate investor", "joint venture"]),
            ("ipo", ["ipo", "public offering"]),
            ("crowdfunding", ["crowdfunding", "kickstarter"]),
        ]

        def classify_funding(val):
            if pd.isna(val):
                return pd.NA, pd.NA
            s = str(val).strip()
            low = s.lower()
            for eid, keywords in funding_keywords:
                if any(k in low for k in keywords):
                    return eid, s
            return "other", s

        results = df["initial_funding_type"].apply(classify_funding)
        df["initial_funding_detail"] = results.apply(lambda x: x[1])
        df["initial_funding_type"] = results.apply(lambda x: x[0])

    # 15. skill_profile → entity domain
    if "skill_profile" in df.columns:
        skill_token_map = {
            "finance": "Finance", "financial": "Finance", "accounting": "Finance",
            "investment": "Finance", "banking": "Finance",
            "tech": "Tech", "technology": "Tech", "engineering": "Tech",
            "software": "Tech", "technical": "Tech", "it": "Tech",
            "ops": "Ops", "operations": "Ops", "operational": "Ops",
            "logistics": "Ops", "supply chain": "Ops", "manufacturing": "Ops",
            "sales": "Sales", "business development": "Sales", "deal-making": "Sales",
            "dealmaking": "Sales",
            "marketing": "Marketing", "advertising": "Marketing",
            "legal": "Legal", "law": "Legal", "regulatory": "Legal",
            "creative": "Creative", "design": "Creative", "artistic": "Creative",
            "media": "Creative",
            "science": "Science", "scientific": "Science", "research": "Science",
            "r&d": "Science",
            "product": "Product", "product management": "Product",
            "brand": "Brand", "branding": "Brand",
        }

        def classify_skill(val):
            if pd.isna(val):
                return pd.NA
            s = str(val).strip()
            low = s.lower()
            # Split on / or ,
            tokens = re.split(r'[/,]', low)
            matched = set()
            for token in tokens:
                token = token.strip()
                for key, canonical in skill_token_map.items():
                    if key in token:
                        matched.add(canonical)
                        break
            if not matched:
                return pd.NA
            sorted_skills = sorted(matched)
            return ",".join(s.lower() for s in sorted_skills)

        df["skill_profile"] = df["skill_profile"].apply(classify_skill)

    # 16. moat_type → entity domain
    if "moat_type" in df.columns:
        moat_keywords = [
            ("brand", ["brand", "franchise"]),
            ("data", ["data", "algorithm", "machine learning", "ai advantage"]),
            ("distribution", ["distribution", "logistics", "supply chain", "retail network"]),
            ("ip", ["ip", "patent", "proprietary", "trade secret", "copyright",
                     "intellectual property"]),
            ("location", ["location", "land", "real estate", "geographic"]),
            ("network_effects", ["network effect", "two-sided", "marketplace effect",
                                  "platform effect"]),
            ("regulatory", ["regulatory", "license", "permit", "fda",
                            "government approval", "compliance", "barrier to entry"]),
            ("reputation", ["reputation", "track record", "trust", "expertise",
                            "relationship", "domain expertise"]),
            ("scale", ["scale", "size", "volume", "cost advantage", "economies"]),
            ("switching_costs", ["switching cost", "lock-in", "integration cost",
                                  "embedded", "entrench"]),
            ("technology", ["technology", "tech advantage", "engineering", "r&d"]),
            ("vertical_integration", ["vertical integrat"]),
        ]

        def classify_moat(val):
            if pd.isna(val):
                return pd.NA, pd.NA
            s = str(val).strip()
            low = s.lower()
            matched = []
            for eid, keywords in moat_keywords:
                if any(k in low for k in keywords):
                    matched.append(eid)
            if not matched:
                return "other", s
            return ",".join(matched), s

        results = df["moat_type"].apply(classify_moat)
        df["moat_type_detail"] = results.apply(lambda x: x[1])
        df["moat_type"] = results.apply(lambda x: x[0])

    # 17. revenue_model_type → entity domain
    if "revenue_model_type" in df.columns:
        revenue_keywords = [
            ("advertising", ["advertising", "ad-supported", "ad revenue", "ad-based"]),
            ("construction", ["construction", "building", "infrastructure",
                              "engineering & construction"]),
            ("diversified", ["diversified", "conglomerate", "holding company",
                             "multi-industry"]),
            ("energy_resources", ["energy", "oil", "gas", "mining", "utility",
                                   "natural resource", "pipeline", "refining"]),
            ("financial_services", ["financial service", "banking", "lending", "credit",
                                     "insurance", "interest income", "wealth management",
                                     "hedge fund"]),
            ("food_beverage", ["food", "restaurant", "beverage", "grocery",
                               "hospitality", "hotel", "resort"]),
            ("healthcare", ["healthcare", "medical", "pharmaceutical", "biotech",
                            "drug", "hospital", "health service"]),
            ("licensing_royalties", ["licensing", "royalt", "franchise fee",
                                      "intellectual property licensing"]),
            ("management_fees", ["management fee", "carried interest", "aum",
                                  "asset management"]),
            ("media_entertainment", ["media", "entertainment", "content", "gaming",
                                      "music", "publishing", "streaming", "film", "sports"]),
            ("platform_marketplace", ["platform", "marketplace", "exchange", "auction"]),
            ("product_sales", ["product sale", "product-based", "consumer product",
                                "cpg", "consumer goods", "packaged goods",
                                "manufacturing and sale"]),
            ("professional_services", ["consulting", "advisory", "professional service",
                                        "legal service", "staffing"]),
            ("real_estate", ["real estate", "property", "reit", "rental income", "leasing"]),
            ("retail", ["retail", "store", "e-commerce", "ecommerce", "online retail",
                        "direct-to-consumer", "dtc"]),
            ("saas_subscription", ["saas", "subscription", "recurring revenue",
                                    "software-as"]),
            ("telecom", ["telecom", "communications", "wireless", "broadband", "cable",
                         "internet service"]),
            ("transaction_commission", ["transaction", "commission", "brokerage",
                                         "payment processing", "interchange"]),
        ]

        def classify_revenue(val):
            if pd.isna(val):
                return pd.NA, pd.NA
            s = str(val).strip()
            low = s.lower()
            matched = []
            for eid, keywords in revenue_keywords:
                if any(k in low for k in keywords):
                    matched.append(eid)
            if not matched:
                return "other", s
            return ",".join(matched), s

        results = df["revenue_model_type"].apply(classify_revenue)
        df["revenue_model_type_detail"] = results.apply(lambda x: x[1])
        df["revenue_model_type"] = results.apply(lambda x: x[0])

    return df


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    wb = openpyxl.load_workbook(XLSX, read_only=True)
    ws = wb.active

    rows = []
    for row in ws.iter_rows(min_row=3, values_only=True):
        row = list(row)
        if len(row) < GAPMINDER_SLUG_COL:
            continue
        person = row[GAPMINDER_SLUG_COL - 1]  # 0-based index
        if not person or str(person).strip() == "":
            continue
        person = str(person).strip()

        record = {"person": person}
        has_data = False
        for col_num, col_name in COLUMNS.items():
            idx = col_num - 1  # 0-based
            if idx < len(row):
                v = row[idx]
                if v is not None and str(v).strip() != "":
                    record[col_name] = v
                    has_data = True
        if has_data:
            rows.append(record)

    wb.close()

    df = pd.DataFrame(rows)
    df = normalize(df)
    # Ensure column order: person first, then alphabetical
    other_cols = sorted([c for c in df.columns if c != "person"])
    df = df[["person"] + other_cols]
    df = df.sort_values("person")

    outpath = os.path.join(OUTPUT_DIR, "ddf--entities--person.csv")
    df.to_csv(outpath, index=False)

    print(f"Wrote {outpath}: {len(df)} rows, {len(other_cols)} fields")
    for col in other_cols:
        filled = df[col].notna().sum()
        print(f"  {col}: {filled}/{len(df)}")

    # Write ethnicity entity domain CSV (one row per base type)
    eth_base_ids = set()
    for val in df["ethnicity"].dropna():
        for token in str(val).split(","):
            token = token.strip()
            if token:
                eth_base_ids.add(token)
    eth_rows = [{"ethnicity": eid, "name": BASE_ETHNICITY_NAMES.get(eid, eid)}
                for eid in sorted(eth_base_ids)]
    eth_df = pd.DataFrame(eth_rows)
    eth_path = os.path.join(OUTPUT_DIR, "ddf--entities--ethnicity.csv")
    eth_df.to_csv(eth_path, index=False)
    print(f"Wrote {eth_path}: {len(eth_df)} ethnicity entities")

    # Write initial_funding_type entity domain CSV
    if "initial_funding_type" in df.columns:
        funding_entities = {
            "self_funded": "Self-Funded",
            "venture_capital": "Venture Capital",
            "angel": "Angel Investment",
            "private_equity": "Private Equity",
            "inheritance": "Inheritance",
            "family_capital": "Family Capital",
            "debt_financing": "Debt Financing",
            "government": "Government/Grant",
            "accelerator": "Accelerator",
            "strategic": "Strategic/Corporate",
            "ipo": "IPO",
            "crowdfunding": "Crowdfunding",
            "other": "Other",
        }
        ft_ids = set(df["initial_funding_type"].dropna().unique())
        ft_rows = [{"initial_funding_type": eid, "name": funding_entities.get(eid, eid)}
                    for eid in sorted(ft_ids)]
        ft_df = pd.DataFrame(ft_rows)
        ft_path = os.path.join(OUTPUT_DIR, "ddf--entities--initial_funding_type.csv")
        ft_df.to_csv(ft_path, index=False)
        print(f"Wrote {ft_path}: {len(ft_df)} funding type entities")

    # Write skill_profile entity domain CSV (one row per base type)
    if "skill_profile" in df.columns:
        skill_name_map = {
            "brand": "Brand", "creative": "Creative", "finance": "Finance",
            "legal": "Legal", "marketing": "Marketing", "ops": "Ops",
            "product": "Product", "sales": "Sales", "science": "Science",
            "tech": "Tech",
        }
        sp_base_ids = set()
        for val in df["skill_profile"].dropna():
            for token in str(val).split(","):
                token = token.strip()
                if token:
                    sp_base_ids.add(token)
        sp_rows = [{"skill_profile": eid,
                     "name": skill_name_map.get(eid, eid.capitalize())}
                    for eid in sorted(sp_base_ids)]
        sp_df = pd.DataFrame(sp_rows)
        sp_path = os.path.join(OUTPUT_DIR, "ddf--entities--skill_profile.csv")
        sp_df.to_csv(sp_path, index=False)
        print(f"Wrote {sp_path}: {len(sp_df)} skill profile entities")

    # Write moat_type entity domain CSV (one row per base type)
    if "moat_type" in df.columns:
        moat_name_map = {
            "brand": "Brand", "data": "Data", "distribution": "Distribution",
            "ip": "IP", "location": "Location", "network_effects": "Network Effects",
            "regulatory": "Regulatory", "reputation": "Reputation", "scale": "Scale",
            "switching_costs": "Switching Costs", "technology": "Technology",
            "vertical_integration": "Vertical Integration", "other": "Other",
        }
        mt_base_ids = set()
        for val in df["moat_type"].dropna():
            for token in str(val).split(","):
                token = token.strip()
                if token:
                    mt_base_ids.add(token)
        mt_rows = [{"moat_type": eid, "name": moat_name_map.get(eid, eid)}
                    for eid in sorted(mt_base_ids)]
        mt_df = pd.DataFrame(mt_rows)
        mt_path = os.path.join(OUTPUT_DIR, "ddf--entities--moat_type.csv")
        mt_df.to_csv(mt_path, index=False)
        print(f"Wrote {mt_path}: {len(mt_df)} moat type entities")

    # Write revenue_model_type entity domain CSV (one row per base type)
    if "revenue_model_type" in df.columns:
        rev_name_map = {
            "advertising": "Advertising", "construction": "Construction",
            "diversified": "Diversified", "energy_resources": "Energy/Resources",
            "financial_services": "Financial Services",
            "food_beverage": "Food/Beverage", "healthcare": "Healthcare",
            "licensing_royalties": "Licensing/Royalties",
            "management_fees": "Management Fees",
            "media_entertainment": "Media/Entertainment",
            "platform_marketplace": "Platform/Marketplace",
            "product_sales": "Product Sales",
            "professional_services": "Professional Services",
            "real_estate": "Real Estate", "retail": "Retail",
            "saas_subscription": "SaaS/Subscription", "telecom": "Telecom",
            "transaction_commission": "Transaction/Commission", "other": "Other",
        }
        rt_base_ids = set()
        for val in df["revenue_model_type"].dropna():
            for token in str(val).split(","):
                token = token.strip()
                if token:
                    rt_base_ids.add(token)
        rt_rows = [{"revenue_model_type": eid, "name": rev_name_map.get(eid, eid)}
                    for eid in sorted(rt_base_ids)]
        rt_df = pd.DataFrame(rt_rows)
        rt_path = os.path.join(OUTPUT_DIR, "ddf--entities--revenue_model_type.csv")
        rt_df.to_csv(rt_path, index=False)
        print(f"Wrote {rt_path}: {len(rt_df)} revenue model type entities")

    # Write 7 enum entity domain CSVs (all valid values, not just those present in data)
    ENUM_DOMAINS = {
        "family_socioeconomic_class": {v: k for k, v in SOCIOECONOMIC_IDS.items()},
        "consumption_index": {v: k for k, v in CONSUMPTION_IDS.items()},
        "market_position": {v: k for k, v in MARKET_POSITION_IDS.items()},
        "media_visibility": {v: k for k, v in MEDIA_VISIBILITY_IDS.items()},
        "parent_education_level": {v: k for k, v in PARENT_EDUCATION_IDS.items()},
        "party": {v: k for k, v in PARTY_IDS.items()},
        "social_media_activity": {v: k for k, v in SOCIAL_MEDIA_IDS.items()},
    }
    for field, id_to_name in ENUM_DOMAINS.items():
        all_ids = sorted(id_to_name.keys())
        rows = [{field: eid, "name": id_to_name[eid]} for eid in all_ids]
        edf = pd.DataFrame(rows)
        epath = os.path.join(OUTPUT_DIR, f"ddf--entities--{field}.csv")
        edf.to_csv(epath, index=False)
        print(f"Wrote {epath}: {len(edf)} entities")


if __name__ == "__main__":
    main()
