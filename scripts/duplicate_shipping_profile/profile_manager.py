"""Manager for duplicating Shopify Delivery (Shipping) Profiles."""
import logging
from typing import Any, Dict, List, Optional

from .models import CreateResult, ProfileSummary

logger = logging.getLogger(__name__)


LIST_QUERY = """
query listDeliveryProfiles {
  deliveryProfiles(first: 50) {
    edges {
      node {
        id
        name
        default
        activeMethodDefinitionsCount
        productVariantsCount { count }
      }
    }
  }
}
"""


DETAIL_QUERY = """
query getDeliveryProfile($id: ID!) {
  deliveryProfile(id: $id) {
    id
    name
    default
    profileLocationGroups {
      locationGroup {
        id
        locations(first: 250) {
          edges { node { id } }
        }
      }
      locationGroupZones(first: 50) {
        edges {
          node {
            zone {
              id
              name
              countries {
                code { countryCode restOfWorld }
                provinces { code }
              }
            }
            methodDefinitions(first: 100) {
              edges {
                node {
                  id
                  name
                  description
                  active
                  rateProvider {
                    __typename
                    ... on DeliveryRateDefinition {
                      price { amount currencyCode }
                    }
                    ... on DeliveryParticipant {
                      carrierService { id name }
                      participantServices { active name }
                      fixedFee { amount currencyCode }
                      percentageOfRateFee
                      adaptToNewServicesFlag
                    }
                  }
                  methodConditions {
                    field
                    operator
                    conditionCriteria {
                      __typename
                      ... on MoneyV2 { amount currencyCode }
                      ... on Weight { unit value }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
"""


CREATE_MUTATION = """
mutation deliveryProfileCreate($profile: DeliveryProfileInput!) {
  deliveryProfileCreate(profile: $profile) {
    profile { id name }
    userErrors { field message }
  }
}
"""


class ShippingProfileManager:
    """Read source delivery profile, transform to input, create a duplicate."""

    def __init__(self, shopify_client):
        self.shopify_client = shopify_client

    def list_profiles(self) -> List[ProfileSummary]:
        """Return all delivery profiles in the shop."""
        response = self.shopify_client.execute_graphql(LIST_QUERY, {})
        if "errors" in response:
            messages = [e.get("message", "Unknown") for e in response["errors"]]
            raise RuntimeError(f"GraphQL errors listing profiles: {', '.join(messages)}")

        edges = response.get("data", {}).get("deliveryProfiles", {}).get("edges", [])
        profiles: List[ProfileSummary] = []
        for edge in edges:
            node = edge.get("node", {}) or {}
            variants_count = (node.get("productVariantsCount") or {}).get("count", 0) or 0
            profiles.append(ProfileSummary(
                id=node.get("id"),
                name=node.get("name", "(unnamed)"),
                default=bool(node.get("default")),
                active_method_definitions_count=int(node.get("activeMethodDefinitionsCount") or 0),
                product_variants_count=int(variants_count),
            ))
        return profiles

    def fetch_profile(self, profile_gid: str) -> Dict[str, Any]:
        """Fetch full source profile structure."""
        response = self.shopify_client.execute_graphql(DETAIL_QUERY, {"id": profile_gid})
        if "errors" in response:
            messages = [e.get("message", "Unknown") for e in response["errors"]]
            raise RuntimeError(f"GraphQL errors fetching profile: {', '.join(messages)}")

        profile = response.get("data", {}).get("deliveryProfile")
        if not profile:
            raise RuntimeError(f"Profile {profile_gid} not found")
        return profile

    def transform_to_input(self, profile_data: Dict[str, Any], new_name: str) -> Dict[str, Any]:
        """Convert deliveryProfile query response → DeliveryProfileInput dict."""
        location_groups_input: List[Dict[str, Any]] = []

        for plg in profile_data.get("profileLocationGroups", []) or []:
            location_group = plg.get("locationGroup") or {}
            location_edges = (location_group.get("locations") or {}).get("edges", []) or []
            location_ids = [edge["node"]["id"] for edge in location_edges if edge.get("node", {}).get("id")]

            zones_input: List[Dict[str, Any]] = []
            zone_edges = (plg.get("locationGroupZones") or {}).get("edges", []) or []
            for zedge in zone_edges:
                znode = zedge.get("node") or {}
                zone = znode.get("zone") or {}
                zones_input.append({
                    "name": zone.get("name", ""),
                    "countries": self._transform_countries(zone.get("countries") or []),
                    "methodDefinitionsToCreate": self._transform_method_definitions(
                        ((znode.get("methodDefinitions") or {}).get("edges") or [])
                    ),
                })

            location_groups_input.append({
                "locations": location_ids,
                "zonesToCreate": zones_input,
            })

        return {
            "name": new_name,
            "locationGroupsToCreate": location_groups_input,
        }

    def _transform_countries(self, countries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Map zone.countries[] → DeliveryCountryInput[]."""
        result: List[Dict[str, Any]] = []
        for country in countries:
            code_obj = country.get("code") or {}
            if code_obj.get("restOfWorld"):
                result.append({"restOfWorld": True})
                continue

            country_code = code_obj.get("countryCode")
            entry: Dict[str, Any] = {}
            if country_code:
                entry["code"] = country_code

            provinces = country.get("provinces") or []
            province_inputs = [{"code": p["code"]} for p in provinces if p.get("code")]
            if province_inputs:
                entry["provinces"] = province_inputs
            else:
                entry["includeAllProvinces"] = True

            if entry:
                result.append(entry)
        return result

    def _transform_method_definitions(self, method_edges: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Map methodDefinitions.edges[] → DeliveryMethodDefinitionInput[]."""
        result: List[Dict[str, Any]] = []
        for medge in method_edges:
            mnode = medge.get("node") or {}
            method_input: Dict[str, Any] = {
                "name": mnode.get("name", ""),
                "description": mnode.get("description"),
                "active": bool(mnode.get("active", True)),
            }

            rate_provider = mnode.get("rateProvider") or {}
            typename = rate_provider.get("__typename")
            if typename == "DeliveryRateDefinition":
                price = rate_provider.get("price") or {}
                method_input["rateDefinition"] = {
                    "price": {
                        "amount": price.get("amount"),
                        "currencyCode": price.get("currencyCode"),
                    }
                }
            elif typename == "DeliveryParticipant":
                carrier_service = rate_provider.get("carrierService") or {}
                participant_input: Dict[str, Any] = {
                    "carrierServiceId": carrier_service.get("id"),
                    "participantServices": rate_provider.get("participantServices") or [],
                    "percentageOfRateFee": rate_provider.get("percentageOfRateFee"),
                    "adaptToNewServices": rate_provider.get("adaptToNewServicesFlag"),
                }
                fixed_fee = rate_provider.get("fixedFee")
                if fixed_fee:
                    participant_input["fixedFee"] = {
                        "amount": fixed_fee.get("amount"),
                        "currencyCode": fixed_fee.get("currencyCode"),
                    }
                method_input["participant"] = {
                    k: v for k, v in participant_input.items() if v is not None
                }

            price_conditions, weight_conditions = self._transform_conditions(
                mnode.get("methodConditions") or []
            )
            if price_conditions:
                method_input["priceConditionsToCreate"] = price_conditions
            if weight_conditions:
                method_input["weightConditionsToCreate"] = weight_conditions

            method_input = {k: v for k, v in method_input.items() if v is not None}
            result.append(method_input)
        return result

    def _transform_conditions(
        self, conditions: List[Dict[str, Any]]
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Split methodConditions[] into (priceConditions, weightConditions)."""
        price_conditions: List[Dict[str, Any]] = []
        weight_conditions: List[Dict[str, Any]] = []
        for cond in conditions:
            criteria = cond.get("conditionCriteria") or {}
            ctypename = criteria.get("__typename")
            operator = cond.get("operator")
            if ctypename == "MoneyV2":
                price_conditions.append({
                    "operator": operator,
                    "criteria": {
                        "amount": criteria.get("amount"),
                        "currencyCode": criteria.get("currencyCode"),
                    },
                })
            elif ctypename == "Weight":
                weight_conditions.append({
                    "operator": operator,
                    "criteria": {
                        "unit": criteria.get("unit"),
                        "value": float(criteria.get("value") or 0),
                    },
                })
        return price_conditions, weight_conditions

    def create_profile(self, profile_input: Dict[str, Any], dry_run: bool = False) -> CreateResult:
        """Execute deliveryProfileCreate mutation (or skip when dry-run)."""
        if dry_run:
            logger.info("[dry-run] Skipping deliveryProfileCreate mutation.")
            return CreateResult(success=True, dry_run=True, new_profile_name=profile_input.get("name"))

        response = self.shopify_client.execute_graphql(
            CREATE_MUTATION,
            {"profile": profile_input},
        )

        if "errors" in response:
            messages = [e.get("message", "Unknown") for e in response["errors"]]
            return CreateResult(success=False, errors=messages)

        payload = (response.get("data") or {}).get("deliveryProfileCreate") or {}
        user_errors = payload.get("userErrors") or []
        if user_errors:
            errors = [
                f"{'/'.join(e.get('field') or [])}: {e.get('message')}" for e in user_errors
            ]
            return CreateResult(success=False, errors=errors)

        created = payload.get("profile") or {}
        return CreateResult(
            success=True,
            new_profile_id=created.get("id"),
            new_profile_name=created.get("name"),
        )

    def duplicate(
        self,
        source_gid: str,
        new_name: Optional[str] = None,
        dry_run: bool = False,
    ) -> CreateResult:
        """End-to-end: fetch source → transform → create."""
        source = self.fetch_profile(source_gid)
        target_name = new_name or f"{source.get('name', 'Profile')} (Copy)"
        profile_input = self.transform_to_input(source, target_name)
        return self.create_profile(profile_input, dry_run=dry_run)
