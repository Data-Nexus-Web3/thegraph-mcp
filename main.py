from mcp.server.fastmcp import FastMCP
import httpx
import json
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Create MCP server instance
mcp = FastMCP("TheGraph MCP Server")

# Base URL for The Graph decentralized network API
THEGRAPH_API_BASE_URL = "https://gateway.thegraph.com/api/"

# The Graph Network subgraph ID (indexes metadata about all subgraphs on the network)
NETWORK_SUBGRAPH_ID = "DZz4kDTdmzWLWsV373w2bSmoar3umKKH9y82SUKr5qmp"

# Retrieve API key from environment variables
API_KEY = os.getenv("THEGRAPH_API_KEY")

# Server-side schema cache: {subgraphId: {"text": str, "json": str}}
_schema_cache = {}


def _sanitize_error(e: Exception) -> str:
    """Strip API key from error messages to prevent leaks."""
    msg = str(e)
    if API_KEY and API_KEY in msg:
        msg = msg.replace(API_KEY, "[REDACTED]")
    return msg


def json_to_graphql_schema(schema_json):
    """Convert JSON schema from introspection to GraphQL text format."""
    types = schema_json["types"]
    schema_text = ""
    
    for t in types:
        if t["kind"] == "OBJECT" and not t["name"].startswith("__"):
            schema_text += f"type {t['name']} {{\n"
            if t["fields"]:
                for f in t["fields"]:
                    field_type = f["type"]
                    type_name = field_type["name"]
                    if field_type["kind"] == "NON_NULL":
                        type_name = f"{field_type['ofType']['name']}!"
                    elif field_type["kind"] == "LIST":
                        type_name = f"[{field_type['ofType']['name']}]"
                    schema_text += f"  {f['name']}: {type_name}\n"
            schema_text += "}\n\n"
    
    return schema_text.strip()

@mcp.tool()
async def getSubgraphSchema(subgraphId: str, asText: bool = False) -> str:
    """Fetch the schema of a specified subgraph using GraphQL introspection.

    Args:
        subgraphId (str): The ID of the subgraph to query.
        asText (bool): If True, return schema as GraphQL text; otherwise, return JSON.

    Returns:
        str: Schema in JSON or GraphQL text format, or an error message.
    """
    # Check cache first
    cached = _schema_cache.get(subgraphId)
    if cached:
        if asText and cached.get("text"):
            return cached["text"]
        if not asText and cached.get("json"):
            return cached["json"]

    if not API_KEY:
        return "API key is required. Set THEGRAPH_API_KEY in your .env file."

    async with httpx.AsyncClient() as client:
        url = f"{THEGRAPH_API_BASE_URL}{API_KEY}/subgraphs/id/{subgraphId}"
        introspection_query = """
        query IntrospectionQuery {
          __schema {
            types {
              name
              kind
              fields {
                name
                type {
                  name
                  kind
                  ofType {
                    name
                    kind
                  }
                }
              }
            }
          }
        }
        """
        try:
            response = await client.post(url, json={"query": introspection_query}, timeout=10)
            response.raise_for_status()
            schema_data = response.json()

            # Fix #4: Check for GraphQL errors at HTTP 200
            if schema_data.get("errors"):
                return f"GraphQL error: {schema_data['errors'][0].get('message', 'Unknown error')}"

            if "data" in schema_data and "__schema" in schema_data["data"]:
                schema = schema_data["data"]["__schema"]
                text_schema = json_to_graphql_schema(schema)
                json_schema = json.dumps(schema)

                # Populate cache with both formats
                _schema_cache[subgraphId] = {"text": text_schema, "json": json_schema}

                return text_schema if asText else json_schema
            else:
                return f"Failed to fetch schema for {subgraphId}"
        except httpx.HTTPError as e:
            return f"Error fetching schema: {_sanitize_error(e)}"
        except Exception as e:
            return f"Error fetching schema: {_sanitize_error(e)}"

@mcp.tool()
async def querySubgraph(subgraphId: str, query: str) -> str:
    """Execute a GraphQL query against a specified subgraph.

    Args:
        subgraphId (str): The ID of the subgraph to query.
        query (str): The GraphQL query string to execute.

    Returns:
        str: Query result in JSON format, or an error message.
    """
    if not API_KEY:
        return "API key is required. Set THEGRAPH_API_KEY in your .env file."
    
    async with httpx.AsyncClient() as client:
        url = f"{THEGRAPH_API_BASE_URL}{API_KEY}/subgraphs/id/{subgraphId}"
        try:
            response = await client.post(url, json={"query": query}, timeout=10)
            response.raise_for_status()
            data = response.json()

            # Fix #4: Check for GraphQL errors at HTTP 200
            if data.get("errors"):
                return f"GraphQL error: {data['errors'][0].get('message', 'Unknown error')}"

            return json.dumps(data)
        except httpx.HTTPError as e:
            return f"Error executing query: {_sanitize_error(e)}"
        except Exception as e:
            return f"Error executing query: {_sanitize_error(e)}"

@mcp.tool()
async def searchSubgraphs(searchQuery: str) -> str:
    """Search for subgraphs on The Graph Network by name or description.

    Args:
        searchQuery (str): The search term to find matching subgraphs.

    Returns:
        str: A concise list of matching subgraphs with their IDs, names, networks, and signal, or an error message.
    """
    if not API_KEY:
        return "API key is required. Set THEGRAPH_API_KEY in your .env file."

    query = """
    query SearchSubgraphs($text: String!) {
      subgraphMetadataSearch(text: $text, first: 20) {
        displayName
        description
        subgraph {
          id
          signalledTokens
          currentVersion {
            metadata {
              description
            }
            subgraphDeployment {
              ipfsHash
              manifest {
                network
                schema {
                  schema
                }
              }
            }
          }
        }
      }
    }
    """

    async with httpx.AsyncClient() as client:
        url = f"{THEGRAPH_API_BASE_URL}{API_KEY}/subgraphs/id/{NETWORK_SUBGRAPH_ID}"
        try:
            response = await client.post(
                url,
                json={"query": query, "variables": {"text": searchQuery}},
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()

            # Fix #4: Check for GraphQL errors at HTTP 200
            if data.get("errors"):
                return f"GraphQL error: {data['errors'][0].get('message', 'Unknown error')}"

            search_results = (data.get("data") or {}).get("subgraphMetadataSearch")
            if not search_results:
                return f"No subgraphs found matching '{searchQuery}'"

            results = []
            for meta in search_results:
                subgraph = meta.get("subgraph")
                if not subgraph or not subgraph.get("currentVersion"):
                    continue

                # Fix #3: Use .get() for id; skip entry if missing
                subgraph_id = subgraph.get("id")
                if not subgraph_id:
                    continue

                version = subgraph.get("currentVersion")
                deployment = version.get("subgraphDeployment")
                if not deployment:
                    continue
                manifest = deployment.get("manifest") or {}
                network = manifest.get("network", "unknown")

                entry = {
                    "subgraphId": subgraph_id,
                    "displayName": meta.get("displayName"),
                    "network": network,
                    "signalledTokens": subgraph.get("signalledTokens", "0"),
                    "deploymentIpfsHash": deployment.get("ipfsHash"),
                }

                desc = meta.get("description")
                version_desc = (version.get("metadata") or {}).get("description")
                if desc:
                    entry["description"] = desc[:150] + "..." if len(desc) > 150 else desc
                elif version_desc:
                    entry["description"] = version_desc[:150] + "..." if len(version_desc) > 150 else version_desc

                # Cache schema for later getSubgraphSchema calls, but strip from response
                schema = (manifest.get("schema") or {}).get("schema")
                if schema:
                    _schema_cache[subgraph_id] = {"text": schema, "json": None}

                results.append(entry)

            if not results:
                return f"Subgraphs were found matching '{searchQuery}', but none have an active deployment. Try a broader search term."

            # Fix #5: Safe sort — handle missing or non-numeric signalledTokens
            def _safe_signal(x):
                try:
                    return int(x.get("signalledTokens") or 0)
                except (ValueError, TypeError):
                    return 0

            results.sort(key=_safe_signal, reverse=True)

            return json.dumps(results)
        except httpx.HTTPError as e:
            return f"Error searching subgraphs: {_sanitize_error(e)}"
        except Exception as e:
            return f"Error searching subgraphs: {_sanitize_error(e)}"


if __name__ == "__main__":
    # Start the MCP server
    mcp.run()
