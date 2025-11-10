from __future__ import annotations

from dataclasses import dataclass
import sqlparse
from sqlparse.sql import Comparison, Token, Where

from typing import Any, Literal

class DirectusQueryBuilder:
    def __init__(self):
        self.query: dict[Literal['query'], dict[str, Any]] = {"query": {}}

    def nested_condition(self, logic_op: str, conditions: list[dict[str, Any]]) -> DirectusQueryBuilder:
        """
        Add nested logical conditions (_and/_or)
        Allows for complex nested conditions
        """
        if "filter" not in self.query["query"]:
            self.query["query"]["filter"] = {}

        # If we already have conditions, wrap everything in a new logical operator
        if self.query["query"]["filter"]:
            current_filter = self.query["query"]["filter"].copy() # type: ignore
            self.query["query"]["filter"] = {
                logic_op: [
                    current_filter,
                    *conditions
                ]
            }
        else:
            self.query["query"]["filter"][logic_op] = conditions

        return self

    def or_condition(self, conditions: list[dict[str, Any]]) -> DirectusQueryBuilder:
        """Add OR conditions"""
        return self.nested_condition("_or", conditions)

    def and_condition(self, conditions: list[dict[str, Any]]) -> DirectusQueryBuilder:
        """Add AND conditions"""
        return self.nested_condition("_and", conditions)

    def field(self, field_name: str, operator: str, value: Any) -> DirectusQueryBuilder:
        """Add a field filter condition"""
        condition = {field_name: {operator: value}}
        return self.and_condition([condition])

    def sort(self, *fields: str) -> DirectusQueryBuilder:
        """
        Add sort conditions. Use '-' prefix for descending order.
        Example:
            .sort('name', '-date_created') # Sort by name ASC, date_created DESC
        """
        if not fields:
            return self

        self.query["query"]["sort"] = list(fields)
        return self

    def limit(self, limit: int) -> DirectusQueryBuilder:
        """
        Set the maximum number of items to return
        Use -1 for maximum allowed items
        """
        self.query["query"]["limit"] = limit
        return self

    def offset(self, offset: int) -> DirectusQueryBuilder:
        """Set the number of items to skip"""
        self.query["query"]["offset"] = offset
        return self

    def page(self, page: int) -> DirectusQueryBuilder:
        """Set the page number (1-indexed)"""
        self.query["query"]["page"] = page
        return self

    def build(self):
        """Build and return the final query"""
        return self.query


@dataclass
class DOp:
    EQUALS = "_eq"
    NOT_EQUALS = "_neq"
    LESS_THAN = "_lt"
    LESS_THAN_EQUAL = "_lte"
    GREATER_THAN = "_gt"
    GREATER_THAN_EQUAL = "_gte"
    IN = "_in"
    NOT_IN = "_nin"
    NULL = "_null"
    NOT_NULL = "_nnull"
    CONTAINS = "_contains"
    NOT_CONTAINS = "_ncontains"
    STARTS_WITH = "_starts_with"
    ENDS_WITH = "_ends_with"
    BETWEEN = "_between"
    NOT_BETWEEN = "_nbetween"
    EMPTY = "_empty"
    NOT_EMPTY = "_nempty"


class SQLToDirectusConverter:
    def __init__(self):
        self.builder = DirectusQueryBuilder()

    def _format_sql(self, sql: str) -> str:
        """Format SQL query before parsing"""
        # Add spaces around parentheses
        sql = sql.replace("(", " ( ")
        sql = sql.replace(")", " ) ")
        # Remove multiple spaces
        sql = " ".join(sql.split())
        return sql

    def _get_next_value_after_keyword(self, tokens: list[Token], keyword: str) -> str | None:
        """Helper to get the next value after a keyword"""
        for i, token in enumerate(tokens):
            if token.ttype is Keyword and token.value.upper() == keyword:
                # Look for the next non-whitespace token
                for next_token in tokens[i+1:]:
                    if not next_token.is_whitespace:
                        return str(next_token)
        return None

    def _get_order_by_fields(self, tokens: list[Token]) -> list[str]:
        """
        Extrait les champs ORDER BY de la requête SQL
        Retourne une liste de champs avec '-' pour DESC
        """
        order_fields = []
        in_order_by = False

        for token in tokens:
            if token.ttype is Keyword and token.value.upper() == "ORDER BY":
                in_order_by = True
                continue

            if in_order_by:
                if token.ttype is Keyword and token.value.upper() in ("LIMIT", "OFFSET"):
                    break

                if not token.is_whitespace and token.value != ',':
                    value = str(token).strip()
                    if value.upper() == "ASC":
                        continue
                    elif value.upper() == "DESC":
                        if order_fields:  # S'assurer qu'il y a un champ précédent
                            order_fields[-1] = f"-{order_fields[-1]}"  # Ajouter le - au champ précédent
                    else:
                        order_fields.append(value)

        return order_fields

    @staticmethod
    def _get_operator_mapping(sql_operator: str) -> str:
        """Map SQL operators to Directus operators"""
        mapping = {
            "=": DOp.EQUALS,
            "!=": DOp.NOT_EQUALS,
            "<": DOp.LESS_THAN,
            "<=": DOp.LESS_THAN_EQUAL,
            ">": DOp.GREATER_THAN,
            ">=": DOp.GREATER_THAN_EQUAL,
            "IN": DOp.IN,
            "NOT IN": DOp.NOT_IN,
            "IS NULL": DOp.NULL,
            "IS NOT NULL": DOp.NOT_NULL,
            "LIKE": DOp.CONTAINS,
        }
        return mapping.get(sql_operator.upper(), sql_operator)

    def _parse_comparison(self, comparison: Comparison) -> dict[str, Any]:
        """Parse a SQL comparison into a Directus filter condition"""
        left = str(comparison.left)
        operator = None
        right_value = None

        # Parcourir les tokens pour trouver l'opérateur et la valeur
        for token in comparison.tokens:
            if token.is_whitespace:
                continue
            if token.ttype is sqlparse.tokens.Keyword:
                operator = self._get_operator_mapping(token.value)
            elif isinstance(token, sqlparse.sql.Parenthesis):
                # Cas spécial pour IN
                values = str(token).strip("()").split(",")
                right_value = [v.strip(" '\"") for v in values]
            elif token.ttype is sqlparse.tokens.Name.Mixed or token.ttype is sqlparse.tokens.String.Single:
                if right_value is None:  # Ne pas écraser la valeur si déjà définie (cas IN)
                    right_value = str(token).strip("'\"")

        if operator is None:
            # Cas où l'opérateur n'est pas un keyword (e.g., =, !=, etc.)
            operator = self._get_operator_mapping(str(comparison.token_next(0)[1]))

        return {left: {operator: right_value}}

    def _parse_group(self, group_token: str) -> dict[str, Any]:
        """Parse a grouped condition token (conditions within parentheses) recursively"""
        # Remove outer parentheses and parse as a separate SQL statement
        group_sql = str(group_token).strip("()")
        if not group_sql.strip():
            return {}

        parsed_group = sqlparse.parse(group_sql)[0]

        conditions = []
        current_operator = "_and"

        # Pour gérer les IN on doit regrouper les tokens
        tokens = [token for token in parsed_group.tokens if not token.is_whitespace]
        i = 0
        while i < len(tokens):
            token = tokens[i]

            if token.ttype is Keyword:
                if token.value.upper() == "OR":
                    current_operator = "_or"
                elif token.value.upper() == "AND":
                    current_operator = "_and"
                i += 1
                continue

            # Détecter si c'est un IN
            if (i + 2) < len(tokens) and tokens[i+1].value.upper() == 'IN':
                # Créer une comparaison artificielle avec les 3 tokens
                comparison = Comparison([tokens[i], tokens[i+1], tokens[i+2]])
                cond = self._parse_comparison(comparison)
                if cond:
                    conditions.append(cond)
                i += 3  # On avance de 3 tokens
                continue

            if isinstance(token, Comparison):
                cond = self._parse_comparison(token)
                if cond:  # Ne pas ajouter les dictionnaires vides
                    conditions.append(cond)
            elif str(token).strip().startswith("("):
                # Parsing récursif pour les sous-groupes
                sub_conditions = self._parse_group(token)
                if sub_conditions:  # Ne pas ajouter les dictionnaires vides
                    conditions.append(sub_conditions)
            elif isinstance(token, sqlparse.sql.Parenthesis):
                if str(token).strip():  # Vérifier que ce n'est pas vide
                    sub_conditions = self._parse_group(token)
                    if sub_conditions:  # Ne pas ajouter les dictionnaires vides
                        conditions.append(sub_conditions)
            else:
                # Pour les tokens complexes, les redécouper
                sub_conditions = self._parse_non_standard_token(token)
                conditions.extend(sub_conditions)

            i += 1

        if not conditions:
            return {}
        if len(conditions) == 1:
            return conditions[0]

        return {current_operator: conditions}

    def _parse_non_standard_token(self, token: str) -> list[dict[str, Any]]:
        """Parse a non-standard token by re-parsing it as SQL"""
        conditions = []
        try:
            sub_tokens = [t for t in sqlparse.parse(str(token))[0].tokens if not t.is_whitespace]
            i = 0
            while i < len(sub_tokens):
                sub_token = sub_tokens[i]

                # Détecter si c'est un IN
                if (i + 2) < len(sub_tokens) and sub_tokens[i+1].value.upper() == 'IN':
                    # Créer une comparaison artificielle avec les 3 tokens
                    comparison = Comparison([sub_tokens[i], sub_tokens[i+1], sub_tokens[i+2]])
                    parsed_condition = self._parse_comparison(comparison)
                    if parsed_condition:
                        conditions.append(parsed_condition)
                    i += 3  # On avance de 3 tokens
                    continue

                if isinstance(sub_token, Comparison):
                    parsed_condition = self._parse_comparison(sub_token)
                    if parsed_condition:
                        conditions.append(parsed_condition)
                i += 1
        except Exception as e:
            print(f"Error in _parse_non_standard_token: {e}")
        return conditions

    def _parse_where_conditions(self, where_clause: Where) -> list[dict[str, Any]]:
        """Parse WHERE clause conditions with support for groups"""
        conditions = []
        current_group = []
        current_operator = "_and"

        for token in where_clause.tokens:
            if token.is_whitespace:
                continue

            if token.ttype is Keyword and token.value.upper() in ("AND", "OR"):
                if token.value.upper() == "OR":
                    current_operator = "_or"
                continue

            if isinstance(token, Comparison):
                cond = self._parse_comparison(token)
                if cond:
                    conditions.append(cond)
            elif str(token).strip().startswith("("):
                group_conditions = self._parse_group(token)
                if group_conditions:
                    conditions.append(group_conditions)
            else:
                # Essayer de parser comme un token complexe
                sub_conditions = self._parse_non_standard_token(token)
                conditions.extend(sub_conditions)

        if current_operator == "_or":
            return [{"_or": conditions}]
        return conditions

    def convert(self, sql_query: str) -> dict[str, Any]:
        """Convert a SQL query to a Directus query"""
        # Format SQL before parsing
        sql_query = self._format_sql(sql_query)

        parsed = sqlparse.parse(sql_query)[0]
        tokens = list(parsed.flatten())

        where_clause = None
        limit_value = None
        offset_value = None

        # Find WHERE clause
        for token in parsed.tokens:
            if isinstance(token, Where):
                where_clause = token
                break

        # Get LIMIT and OFFSET values
        limit_str = self._get_next_value_after_keyword(tokens, "LIMIT")
        offset_str = self._get_next_value_after_keyword(tokens, "OFFSET")

        if limit_str and limit_str.isdigit():
            limit_value = int(limit_str)
        if offset_str and offset_str.isdigit():
            offset_value = int(offset_str)

        # Build the query using DirectusQueryBuilder
        builder = DirectusQueryBuilder()

        # Add WHERE conditions if present
        if where_clause:
            conditions = self._parse_where_conditions(where_clause)
            builder.and_condition(conditions)

        # Add ORDER BY if present
        order_fields = self._get_order_by_fields(tokens)
        if order_fields:
            builder.sort(*order_fields)

        # Add limit and offset if present
        if limit_value is not None:
            builder.limit(limit_value)
        if offset_value is not None:
            builder.offset(offset_value)

        return builder.build()