#!/usr/bin/env python3
"""
MCP Server for embedding-based matching between Hurun and Forbes billionaire lists.
"""

import asyncio
import json
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any
import numpy as np
from sentence_transformers import SentenceTransformer
import pickle
from sklearn.metrics.pairwise import cosine_similarity
import mcp.server.stdio
import mcp.types as types
from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions


class EmbeddingMatcher:
    def __init__(self):
        self.model = SentenceTransformer("multi-qa-mpnet-base-dot-v1")
        self.hurun_data = None
        self.forbes_data = None
        self.embeddings = None
        self.metadata = None
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
                print(f"Loaded {len(self.metadata)} billionaire embeddings")
            else:
                print(f"Embeddings not found at {embeddings_path}")
        except Exception as e:
            print(f"Error loading embeddings: {e}")

    def get_wealth_data(self, person_id: str, source: str) -> Dict[str, Any]:
        """Get average wealth data for a person from the last 3 years."""
        wealth_data = {"average_wealth": None, "wealth_years": []}

        try:
            if source == "hurun":
                wealth_path = Path(__file__).parent.parent / "intermediate/hurun/ddf--datapoints--wealth--by--person--year.csv"
            else:  # forbes
                wealth_path = Path(__file__).parent.parent / "intermediate/forbes/ddf--datapoints--worth--by--person--year.csv"

            if wealth_path.exists():
                wealth_df = pd.read_csv(wealth_path)
                person_wealth = wealth_df[wealth_df["person"] == person_id]

                if not person_wealth.empty:
                    # Get latest 3 years of data
                    person_wealth = person_wealth.sort_values("year", ascending=False).head(3)
                    wealth_data["wealth_years"] = person_wealth[
                        ["year", "worth" if source == "forbes" else "wealth"]
                    ].to_dict("records")
                    # Calculate average wealth
                    wealth_column = "worth" if source == "forbes" else "wealth"
                    wealth_data["average_wealth"] = person_wealth[wealth_column].mean()
        except Exception as e:
            print(f"Error loading wealth data: {e}")

        return wealth_data

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

    def embedding_search(self, person_id: str, source: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Search for similar billionaires using embedding similarity.

        Args:
            person_id: The person ID to search for
            source: The source dataset ("hurun" or "forbes")
            limit: Maximum number of results to return

        Returns:
            List of matches with detailed information including demographics and wealth data
        """
        if self.embeddings is None or self.metadata is None:
            return []

        # Get the person's data from the appropriate dataset
        if source == "hurun" and self.hurun_data is not None:
            person_data = self.hurun_data[self.hurun_data["person"] == person_id]
        elif source == "forbes" and self.forbes_data is not None:
            person_data = self.forbes_data[self.forbes_data["person"] == person_id]
        else:
            return []

        if person_data.empty:
            return []

        person_row = person_data.iloc[0]

        # Create query profile
        query_profile = self.create_query_profile(
            name=person_row.get("name"),
            country=person_row.get("country"),
            company=person_row.get("company"),
            birth_year=person_row.get("birth_year"),
            industry=person_row.get("industry"),
            gender=person_row.get("gender")
        )

        # Generate query embedding
        query_embedding = self.model.encode(query_profile)

        # Calculate cosine similarities
        similarities = cosine_similarity([query_embedding], self.embeddings)[0]

        # Get top matches
        top_indices = np.argsort(similarities)[-limit:][::-1]

        results = []
        for idx in top_indices:
            match = self.metadata[idx]
            match_person_id = match["person_id"]
            match_source = match["source"]

            # Get detailed person data
            if match_source == "hurun" and self.hurun_data is not None:
                person_row = self.hurun_data[self.hurun_data["person"] == match_person_id]
            elif match_source == "forbes" and self.forbes_data is not None:
                person_row = self.forbes_data[self.forbes_data["person"] == match_person_id]
            else:
                continue

            if person_row.empty:
                continue

            person_row = person_row.iloc[0]

            # Get wealth data
            wealth_data = self.get_wealth_data(match_person_id, match_source)

            results.append({
                "source": match_source,
                "name": person_row.get("name", ""),
                "person_id": match_person_id,
                "similarity_score": float(similarities[idx]),
                "birth_year": person_row.get("birth_year", None),
                "gender": person_row.get("gender", None),
                "country": person_row.get("country", None),
                "industry": person_row.get("industry", None),
                "company": person_row.get("company", None),
                "title": person_row.get("title", None),
                "average_wealth": wealth_data["average_wealth"],
                "wealth_history": wealth_data["wealth_years"],
            })

        return results


# Initialize the embedding matcher
embedding_matcher = EmbeddingMatcher()

# Create the MCP server
server = Server("embedding-matcher")


@server.list_tools()
async def handle_list_tools() -> List[types.Tool]:
    """List available tools."""
    return [
        types.Tool(
            name="embedding_search",
            description="Search for similar billionaires using embedding similarity based on a person ID from either Hurun or Forbes datasets. Returns detailed information including demographics, company info, and latest wealth data.",
            inputSchema={
                "type": "object",
                "properties": {
                    "person_id": {"type": "string", "description": "The person ID to search for"},
                    "list": {"type": "string", "description": "The source dataset: either 'hurun' or 'forbes'"},
                    "limit": {
                        "type": "integer",
                        "description": "Total number of results to return (default: 10)",
                        "default": 10,
                    },
                },
                "required": ["person_id", "list"],
            },
        )
    ]


@server.call_tool()
async def handle_call_tool(name: str, arguments: dict) -> List[types.TextContent]:
    """Handle tool calls."""
    if name == "embedding_search":
        person_id = arguments.get("person_id", "")
        source = arguments.get("list", "")
        limit = arguments.get("limit", 10)

        if not person_id:
            return [types.TextContent(type="text", text="Error: person_id parameter is required")]

        if source not in ["hurun", "forbes"]:
            return [types.TextContent(type="text", text="Error: list parameter must be either 'hurun' or 'forbes'")]

        try:
            results = embedding_matcher.embedding_search(person_id, source, limit)

            # Format results as JSON
            response = {
                "query_person_id": person_id,
                "query_source": source,
                "matches": results,
                "total_matches": len(results)
            }

            return [types.TextContent(type="text", text=json.dumps(response, indent=2))]

        except Exception as e:
            return [types.TextContent(type="text", text=f"Error performing search: {str(e)}")]

    else:
        return [types.TextContent(type="text", text=f"Unknown tool: {name}")]


async def main():
    """Run the MCP server."""
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="embedding-matcher",
                server_version="1.0.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )


if __name__ == "__main__":
    asyncio.run(main())
