# Methodology for Collecting and Merging Billionaire Data

## Introduction

The goal of this project is to compile a comprehensive dataset of billionaires by combining data from two major sources: the Forbes Billionaires List (published by Forbes in the United States) and the Hurun Rich List (published by Hurun in China). By merging these lists, we aim to create a more inclusive and global view of billionaire data. Both lists are reported annually, so we will focus on historical data across years. Key challenges include inconsistent naming, data discrepancies (e.g., differing citizenship attributions for individuals with multiple nationalities), and ensuring uniqueness. We will leverage large language models (LLMs) to assist with summarization and merging.

## Step 1: Data Collection

- Gather historical billionaire lists from Forbes and Hurun.
- Collect data year by year to capture changes over time (e.g., net worth fluctuations, new entrants).
- Sources may include official websites, APIs, or archived datasets—ensure data is scraped or downloaded ethically and legally.

## Step 2: Data Cleaning

- **Handle Inconsistencies in Each List:** Names may vary due to transliterations, abbreviations, or formatting (e.g., "Elon Musk" vs. "Musk, Elon"). Standardize names, ages, net worth, and other attributes.
- **Assign Unique IDs:** For each list independently, create a unique identifier for every billionaire. This could be based on a combination of name, birthdate, country, and other stable attributes to avoid duplicates within the same list.
- Remove any invalid or incomplete entries.
### Hurun-Specific Cleaning Issues

- **Handling Duplicate-Like Entries**: During the processing of the Hurun list, it was noted that some entries might appear to be duplicates but represent different entities (e.g., `zong_qinghou_family` and `zong_qinghou`). For now, these will be treated as distinct entities. The final decision on how to handle such cases will be made during the merging phase with the Forbes list.


- **No Unique IDs:** The Hurun lists do not provide inherent unique identifiers for individuals. We will generate these programmatically, similar to the general approach, using a hash or combination of stable attributes like full name, birth year, and other available details.

- **Format Differences Across Years:** 
  - Data from 2019-2025 follows a consistent format.
  - Pre-2019 data uses a different format and often lacks country information for individuals. To address this, we will impute missing country data where possible by cross-referencing with later Hurun entries, Forbes data, or using LLMs to infer based on other attributes (e.g., name origins or known residences). If imputation is unreliable, flag these entries for manual review.

### Forbes-Specific Cleaning Issues

- **Unique IDs via URIs:** Forbes provides a URI for each person's page on their website, which we assume remains stable over time. Use this URI as the unique identifier for individuals in the Forbes list.

- **Junk Data Removal:** The list contains some garbage data identifiable by net worth values that are floating-point numbers. Remove any entries where the net worth is a floating-point number.

## Step 3: Merging the Lists

### Embedding-Based Matching Workflow

Based on experimental results with embedding similarity and MCP tools, we have developed a simplified approach:

### Stage 1: LLM-Assisted Automated Mapping

**1.1 Candidate Generation via MCP Server**
- **MCP Server:** We've built an MCP server that uses embedding similarity instead of simple fuzzy matching
  - For each person in the Hurun list, query the MCP server using their comprehensive profile (name, country, company, birth year, industry, gender)
  - The server returns ranked candidates from both Hurun and Forbes datasets based on semantic similarity
  - Typical search returns top 10 candidates with similarity scores

**1.2 LLM Reasoning for Match Decision**
- Present the query person's profile and candidate matches to an LLM
- The LLM analyzes all available information (demographics, business context, wealth patterns, name variations) to determine matches
- LLM provides reasoning for each decision, handling complex cases like:
  - Cross-language names (Ma Yun ↔ Jack Ma)
  - Family vs individual entries ("& Family" suffixes)
  - Transliteration variations
  - Business evolution over time

**1.3 Cluster Merging Logic**
- Track all mapped IDs to prevent conflicts
- If a new mapping includes IDs already present in previous mappings, merge the clusters automatically
- This solves mutual matching cases (e.g., Ma Yun → Jack Ma and Jack Ma → Ma Yun both return similar candidates)
- Maintain a graph structure where nodes are person IDs and edges represent "same person" relationships

### Stage 2: Manual Review of Unmatched Entries

**2.1 Identify Single-List Entries**
- Extract all person IDs that appear only in Hurun or only in Forbes after Stage 1
- These represent either:
  - Genuinely unique individuals only covered by one source
  - Missed matches due to data quality issues or extreme name variations

**2.2 Manual Review Process**
- Review unmatched entries with high wealth values or notable profiles
- Use additional research (web search, news articles) to verify if they appear in the other dataset under different names
- Flag ambiguous cases for further investigation

### Findings from Experimental Results

**Bidirectional Search Advantage:**
- Our experiments showed that search direction matters (Forbes→Hurun worked better than Hurun→Forbes for Ma Yun/Jack Ma)
- The workflow processes all entries to capture matches in both directions

**Embedding Model Selection:**
- Using `multi-qa-mpnet-base-dot-v1` based on performance testing
- Creates comprehensive profiles including all available demographic and business information
- Handles semantic similarity beyond simple name matching

**Cross-Language Handling:**
- Successfully tested with Ma Yun (Hurun) ↔ Jack Ma (Forbes) case
- LLM reasoning helps bridge language gaps that pure embedding similarity might miss

### Expected Outcomes

Based on experimental validation:
- **High-confidence matches:** Embedding similarity > 0.8 + same company → ~95% accuracy
- **Cross-language cases:** Handled through LLM reasoning with context
- **Complex cases:** Family relationships and name variations resolved through comprehensive profiling
- **Coverage:** Expect 70-80% automatic matching, 20-30% requiring manual review

This approach uses the findings from experimentation.

### Defining a Unified Unique ID

To ensure every entity in the final merged dataset has a stable and unique identifier, we will adopt the following strategy:

1.  **Base ID Generation**: A "slug" is created from the person's or entity's name. This process involves:
    -   Converting the name to lowercase.
    -   Replacing spaces and special characters with underscores (`_`).
    -   Examples: "Elon Musk" becomes `elon_musk`; "Wessels Family" becomes `wessels_family`.

2.  **Uniqueness Enforcement**:
    -   The first time a name is encountered, its unique ID is the generated slug (e.g., `john_smith`).
    -   If another entity with the same name is found, a numeric suffix is appended to ensure uniqueness. The second instance will be `john_smith_2`, the third `john_smith_3`, and so on.

This approach is simple, robust, and accommodates both individual billionaires and group entities without relying on potentially missing or inconsistent data like birth years or citizenship.

## Role of Large Language Models

- Employ LLMs to:
  - Summarize and analyze individual lists for patterns or anomalies.
  - Assist in automated merging by generating match probabilities, suggesting resolutions for conflicts, or flagging potential duplicates.
- This will enhance efficiency, especially for large-scale historical data.

## Potential Challenges and Mitigations

- **Data Privacy and Ethics:** Ensure compliance with data usage policies from Forbes and Hurun.
- **Accuracy:** Validate merged data against additional sources if possible.
- **Scalability:** Process data in batches for efficiency.

This methodology will evolve as we implement and test the process.
