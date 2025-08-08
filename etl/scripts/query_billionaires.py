#!/usr/bin/env python3
"""
Script to query billionaire data using pre-generated embeddings.
"""

import pandas as pd

import pandas as pd
from pathlib import Path
from sentence_transformers import SentenceTransformer
import pickle
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import json
from pathlib import Path


class BillionaireQuery:
    def __init__(self):
        self.model = SentenceTransformer("multi-qa-mpnet-base-dot-v1")
        self.embeddings = None
        self.metadata = None
        self.hurun_data = None
        self.forbes_data = None
        self.load_data()
        self.load_embeddings()

    def load_data(self):
        """Load Hurun and Forbes data from CSV files."""
        try:
            # Load Hurun data
            hurun_path = Path(__file__).parent.parent / "intermediate/hurun/ddf--entities--person.csv"
            if hurun_path.exists():
                self.hurun_data = pd.read_csv(hurun_path).replace({np.nan: None})
                print(f"Loaded {len(self.hurun_data)} Hurun entries")
            else:
                print(f"Hurun data not found at {hurun_path}")

            # Load Forbes data
            forbes_path = Path(__file__).parent.parent / "intermediate/forbes/ddf--entities--person.csv"
            if forbes_path.exists():
                self.forbes_data = pd.read_csv(forbes_path).replace({np.nan: None})
                print(f"Loaded {len(self.forbes_data)} Forbes entries")
            else:
                print(f"Forbes data not found at {forbes_path}")

        except Exception as e:
            print(f"Error loading data: {e}")

    def load_embeddings(self):
        """Load pre-generated embeddings and metadata."""
        try:
            embeddings_path = Path(__file__).parent.parent / "intermediate/embeddings/billionaire_embeddings.pkl"
            if embeddings_path.exists():
                with open(embeddings_path, "rb") as f:
                    data = pickle.load(f)
                    self.embeddings = data["embeddings"]
                    self.metadata = data["metadata"]
                print(f"Loaded {len(self.metadata)} billionaire profiles")
            else:
                print(f"Embeddings not found at {embeddings_path}")
        except Exception as e:
            print(f"Error loading embeddings: {e}")

    def create_query_profile(self, name=None, country=None, company=None, birth_year=None, industry=None, gender=None):
        """Create a query profile string from provided information."""
        profile_parts = []

        if name:
            profile_parts.append(f"Name: {name}")
        else:
            profile_parts.append("Name: n/a")
        if country:
            profile_parts.append(f"Country: {country}")
        else:
            profile_parts.append("Country: n/a")
        if company:
            profile_parts.append(f"Company: {company}")
        else:
            profile_parts.append("Company: n/a")
        if birth_year:
            profile_parts.append(f"Birth Year: {int(birth_year)}")
        else:
            profile_parts.append("Birth Year: n/a")
        if industry:
            profile_parts.append(f"Industry: {industry}")
        else:
            profile_parts.append("Industry: n/a")
        if gender:
            profile_parts.append(f"Gender: {gender}")
        else:
            profile_parts.append("Gender: n/a")

        return " ".join(profile_parts)

    def query_billionaires(
        self, name=None, country=None, company=None, birth_year=None, industry=None, gender=None, limit=5
    ):
        """Query billionaires based on provided information."""
        if not any([name, country, company, birth_year, industry]):
            print("Please provide at least one search parameter")
            return []

        query_profile = self.create_query_profile(name, country, company, birth_year, industry)
        query_embedding = self.model.encode(query_profile)

        # Calculate cosine similarities
        similarities = cosine_similarity([query_embedding], self.embeddings)[0]

        # Get top matches
        top_indices = np.argsort(similarities)[-limit:][::-1]

        results = []
        for idx in top_indices:
            match = self.metadata[idx]
            results.append(
                {
                    "source": match["source"],
                    "name": match["name"],
                    "person_id": match["person_id"],
                    "similarity_score": float(similarities[idx]),
                    "profile": match["profile"],
                }
            )

        return results


if __name__ == "__main__":
    query = BillionaireQuery()

    # Example query using data from Hurun where the id is elon_musk
    if query.hurun_data is not None:
        elon_musk = query.hurun_data[query.hurun_data["person"] == "madhusudan_agarwal"]
        if not elon_musk.empty:
            elon_musk = elon_musk.iloc[0]
            results = query.query_billionaires(
                name=elon_musk["name"],
                country=elon_musk["country"],
                industry=elon_musk["industry"],
                birth_year=elon_musk["birth_year"],
                gender=elon_musk["gender"],
                company=elon_musk["company"],
                limit=10
            )
            print(json.dumps(results, indent=2, ensure_ascii=False))
        else:
            print("Elon Musk not found in Hurun data")
    else:
        print("Hurun data not loaded")

    # print(json.dumps(results, indent=2))
