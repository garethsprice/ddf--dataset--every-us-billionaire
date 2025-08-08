#!/usr/bin/env python3
"""
Script to generate and store vector representations of billionaire profiles.
"""

import pandas as pd
from pathlib import Path
from sentence_transformers import SentenceTransformer
import pickle
import numpy as np


class EmbeddingGenerator:
    def __init__(self):
        self.model = SentenceTransformer("multi-qa-mpnet-base-dot-v1")
        self.hurun_data = None
        self.forbes_data = None
        self.load_data()

    def load_data(self):
        """Load Hurun and Forbes data from CSV files."""
        try:
            # Load Hurun data
            hurun_path = (
                Path(__file__).parent.parent / "intermediate/hurun/ddf--entities--person.csv"
            )
            if hurun_path.exists():
                self.hurun_data = pd.read_csv(hurun_path)
                print(f"Loaded {len(self.hurun_data)} Hurun entries")
            else:
                print(f"Hurun data not found at {hurun_path}")

            # Load Forbes data
            forbes_path = (
                Path(__file__).parent.parent / "intermediate/forbes/ddf--entities--person.csv"
            )
            if forbes_path.exists():
                self.forbes_data = pd.read_csv(forbes_path)
                print(f"Loaded {len(self.forbes_data)} Forbes entries")
            else:
                print(f"Forbes data not found at {forbes_path}")

        except Exception as e:
            print(f"Error loading data: {e}")

    def create_profile(self, row, source):
        """Create a comprehensive profile string for a billionaire."""
        profile_parts = []

        # Basic information
        profile_parts.append(f"Billionaire Name: {row.get('name', '')}")
        if source == "hurun" and "chinese_name" in row and pd.notna(row["chinese_name"]):
            profile_parts.append(f"Chinese Name: {row['chinese_name']}")
        if "last_name" in row and pd.notna(row["last_name"]):
            profile_parts.append(f"Last Name: {row['last_name']}")
        if "age" in row and pd.notna(row["age"]):
            profile_parts.append(f"Age: {int(row['age'])}")
        if "birth_year" in row and pd.notna(row["birth_year"]):
            profile_parts.append(f"Birth Year: {int(row['birth_year'])}")
        if "gender" in row and pd.notna(row["gender"]):
            profile_parts.append(f"Gender: {row['gender']}")
        if "country" in row and pd.notna(row["country"]):
            profile_parts.append(f"Country: {row['country']}")

        # Professional information
        if "industry" in row and pd.notna(row["industry"]):
            profile_parts.append(f"Industry: {row['industry']}")
        if "company" in row and pd.notna(row["company"]):
            profile_parts.append(f"Company: {row['company']}")
        if source == "hurun" and "headquarter" in row and pd.notna(row["headquarter"]):
            profile_parts.append(f"Headquarter: {row['headquarter']}")
        if source == "forbes" and "title" in row and pd.notna(row["title"]):
            profile_parts.append(f"Title: {row['title']}")

        return "\n".join(profile_parts)

    def generate_embeddings(self):
        """Generate embeddings for all billionaire profiles."""
        embeddings = []
        metadata = []

        # Process Hurun data
        if self.hurun_data is not None:
            for _, row in self.hurun_data.iterrows():
                profile = self.create_profile(row, "hurun")
                embedding = self.model.encode(profile)
                embeddings.append(embedding)
                metadata.append(
                    {
                        "source": "hurun",
                        "person_id": row.get("person", ""),
                        "name": row.get("name", ""),
                        "profile": profile,
                    }
                )

        # Process Forbes data
        if self.forbes_data is not None:
            for _, row in self.forbes_data.iterrows():
                profile = self.create_profile(row, "forbes")
                embedding = self.model.encode(profile)
                embeddings.append(embedding)
                metadata.append(
                    {
                        "source": "forbes",
                        "person_id": row.get("person", ""),
                        "name": row.get("name", ""),
                        "profile": profile,
                    }
                )

        # Save embeddings and metadata
        output_path = Path(__file__).parent.parent / "intermediate/embeddings"
        output_path.mkdir(parents=True, exist_ok=True)

        with open(output_path / "billionaire_embeddings.pkl", "wb") as f:
            pickle.dump({"embeddings": np.array(embeddings), "metadata": metadata}, f)

        print(f"Generated and saved {len(embeddings)} embeddings")


if __name__ == "__main__":
    generator = EmbeddingGenerator()
    generator.generate_embeddings()
