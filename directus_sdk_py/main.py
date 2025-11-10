from __future__ import annotations

import requests
from requests import HTTPError
from urllib.parse import urljoin, urlparse
import urllib3
from urllib3.exceptions import InsecureRequestWarning
from typing import Any, Literal, overload
import json
from mimetypes import guess_type

class DirectusClient:
    def __init__(self, url: str, 
                 token: str | None = None, 
                 email: str | None = None, 
                 password: str | None = None, 
                 verify: bool = False):
        """
        Initialize the DirectusClient.

        Args:
            url (str): The URL of the Directus instance.
            token (str): The static token for authentication (optional).
            email (str): The email for authentication (optional).
            password (str): The password for authentication (optional).
            verify (bool): Whether to verify SSL certificates (default: False).
        """
        self.verify = verify
        if not self.verify:
            urllib3.disable_warnings(category=InsecureRequestWarning)
        
        # Store path component of base url if provided
        _url = urlparse(url)
        self.url = f"{_url.scheme}://{_url.netloc}"
        self.subpath = _url.path.strip('/') if _url.path != '/' else None

        # Set defaults for instance properties
        self.expires: str = ""

        # Use provided token for auth
        self.static_token = token or ""
        self.temporary_token = ""
        
        # If email and password are provided, get token from those
        self.email = email or ""
        self.password = password or ""
        if email and password:
            self._login()
            
    @property
    def token_header(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.get_token()}"}

    @property
    def email_auth(self) -> dict[str, str]:
        return {"email": self.email, "password": self.password}

    def _login(self) -> None:
        """ Login with the /auth/login endpoint using provided email and password
        """
        auth = requests.post(
            f"{self.url}/auth/login",
            json=self.email_auth,
            verify=self.verify
        )
        auth.raise_for_status()
        _json = auth.json()
        if 'errors' in _json:
            raise HTTPError({"errors": _json['errors']})

        _data: dict[str, Any] = _json['data']
        self.static_token = ""
        self.temporary_token: str = _data['access_token']
        self.refresh_token: str = _data['refresh_token']
        self.expires: str = _data['expires']
    
    def logout(self) -> None:
        """Logout using the /auth/logout endpoint."""
        requests.post(
            f"{self.url}/auth/logout",
            headers=self.token_header,
            json={"refresh_token": self.refresh_token},
            verify=self.verify
        )
        self.temporary_token = ""
        self.refresh_token = ""

    def refresh(self, refresh_token: str | None = None) -> None:
        """
        Retrieve new temporary access token and refresh token.

        Args:
            refresh_token (str): The refresh token (optional).
        """
        auth = requests.post(
            f"{self.url}/auth/refresh",
            json={"refresh_token": refresh_token, 'mode': 'json'},
            verify=self.verify
        )
        auth.raise_for_status()
        _json = auth.json()
        _data = _json['data']
        self.temporary_token = _data['access_token']
        self.refresh_token = _data['refresh_token']
        self.expires = _data['expires']

    def get_token(self):
        """
        Get the authentication token.

        Returns:
            str: The authentication token.
        """
        # Resolve token static -> temporary -> empty
        return self.static_token or self.temporary_token or ""
    
    def clean_url(self, domain: str, path: str) -> str:
        """
        Clean the URL by removing any leading or trailing slashes.

        Args:
            path (str): The URL path.

        Returns:
            str: The cleaned URL path.
        """
        
        # Strip slashes
        path = path.strip('/')
        
        # Add subpath
        if self.subpath is not None:
            path = f"{self.subpath}/{path}"
        
        # Join to domain
        return urljoin(domain, path)
    
    # No good way to overload this since **kwargs could technically change the type.
    # Explicitly define output type for all internal calls
    @overload
    def get(self, path: str, output_type: Literal['csv']='csv', **kwargs: Any) -> str: ... # pyright: ignore[reportOverlappingOverload]
    @overload
    def get(self, path: str, output_type: Literal['json']='json', **kwargs: Any) -> dict[str, Any] | list[Any]: ...
    def get(self, path: str, output_type: Literal['json', 'csv'] = 'json', **kwargs: Any) -> dict[str, Any] | list[Any] | str:
        """
        Perform a GET request to the specified path.

        Args:
            path (str): The API endpoint path.
            output_type (str): The output type (default: "json").
            **kwargs: Additional keyword arguments to pass to the request.

        Returns:
            dict or str: The response data.
        """
        data = requests.get(
            self.clean_url(self.url, path),
            headers=self.token_header,
            verify=self.verify,
            **kwargs
        )
        data.raise_for_status()
        if output_type == 'csv':
            return data.text
        return data.json()['data']

    def post(self, path: str, **kwargs: Any):
        """
        Perform a POST request to the specified path.

        Args:
            path (str): The API endpoint path.
            **kwargs: Additional keyword arguments to pass to the request.

        Returns:
            dict: The response data.
        """
        response = requests.post(
            self.clean_url(self.url, path),
            headers=self.token_header,
            verify=self.verify,
            **kwargs
        )
        if response.status_code != 200:
            raise HTTPError(response.text)

        return response.json()

    def search(self, path: str, query: dict[str, str] | None, **kwargs: Any) -> dict[str, Any] | list[Any]:
        """
        Perform a SEARCH request to the specified path.

        Args:
            path (str): The API endpoint path.
            query (dict): The search query parameters (optional).
            **kwargs: Additional keyword arguments to pass to the request.

        Returns:
            dict: The response data.
        """
        headers = self.token_header
        response = requests.request(
            "SEARCH", 
            self.clean_url(self.url, path), 
            headers=headers, 
            json=query, 
            verify=self.verify,
            **kwargs)
        response.raise_for_status()
        return response.json()['data']

    def delete(self, path: str, **kwargs: Any):
        """
        Perform a DELETE request to the specified path.

        Args:
            path (str): The API endpoint path.
            **kwargs: Additional keyword arguments to pass to the request.
        """
        response = requests.delete(
            self.clean_url(self.url, path),
            headers=self.token_header,
            verify=self.verify,
            **kwargs
        )
        if response.status_code != 204:
            raise HTTPError(response.text)

    def patch(self, path: str, **kwargs: Any):
        """
        Perform a PATCH request to the specified path.

        Args:
            path (str): The API endpoint path.
            **kwargs: Additional keyword arguments to pass to the request.

        Returns:
            dict: The response data.
        """
        response = requests.patch(
            self.clean_url(self.url, path),
            headers=self.token_header,
            verify=self.verify,
            **kwargs
        )

        if response.status_code not in [200, 204]:
            raise HTTPError(response.text)

        return response.json()

    def me(self) -> dict[str, Any]:
        """
        Get the current user.

        Returns:
            dict: The user data.
        """
        return dict(self.get("/users/me", output_type='json'))
    
    def get_users(self, query: dict[str, str] | None, **kwargs: Any):
        """
        Get users based on the provided query.

        Args:
            query (dict): The search query parameters (optional).
            **kwargs: Additional keyword arguments to pass to the request.

        Returns:
            list: The list of users matching the query.
        """
        return self.search("/users", query=query, **kwargs)

    def create_user(self, user_data: dict[str, Any], **kwargs: Any):
        """
        Create a new user.

        Args:
            user_data (dict): The user data.
            **kwargs: Additional keyword arguments to pass to the request.

        Returns:
            dict: The created user.
        """
        return self.post("/users", json=user_data, **kwargs)

    def update_user(self, user_id: int, user_data: dict[str, Any], **kwargs: Any):
        """
        Update a user.

        Args:
            user_id (str): The user ID.
            user_data (dict): The updated user data.
            **kwargs: Additional keyword arguments to pass to the request.

        Returns:
            dict: The updated user.
        """
        return self.patch(f"/users/{user_id}", json=user_data, **kwargs)

    def delete_user(self, user_id: int, **kwargs: Any):
        """
        Delete a user.

        Args:
            user_id (str): The user ID.
            **kwargs: Additional keyword arguments to pass to the request.
        """
        self.delete(f"/users/{user_id}", **kwargs)

    def get_files(self, query: dict[str, str] | None, **kwargs: Any):
        """
        Get files based on the provided query.

        Args:
            query (dict): The search query parameters (optional).
            **kwargs: Additional keyword arguments to pass to the request.

        Returns:
            list: The list of files matching the query.
        """
        return self.search("/files", query=query, **kwargs)

    def retrieve_file(self, file_id: str, **kwargs: Any) -> str | bytes:
        """
        Retrieve information about a file, not the way to download it

        Args:
            file_id (str): The file ID.
            **kwargs: Additional keyword arguments to pass to the request.

        Returns:
            str or bytes: The file content.
        """
        url = f"{self.url}/files/{file_id}"
        headers = self.token_header
        response = requests.get(url, headers=headers, verify=self.verify, **kwargs)
        if response.status_code != 200:
            raise HTTPError(response.text)
        return response.content

    def download_file(self, file_id: str, file_path: str) -> None:
        """
        Just download a directus file in local
        Args:
            file_id (str): The file ID.
            file_path (str): The path to save the file on your computer / server.
        """
        url = f"{self.url}/assets/{file_id}?download="
        headers = self.token_header
        response = requests.get(url, headers=headers)
    
        
        if response.status_code != 200:
            raise HTTPError(response.text)
        with open(file_path, "wb") as file:
            file.write(response.content)
    
    def download_photo(self, file_id: str, file_path: str, 
                       display: dict[str, Any] | None = None, 
                       transform: list[Any] | None = None) -> None:
        """
        Download a file from Directus.

        Args:
            file_id (str): The file ID.
            file_path (str): The path to save the file.
            display (dict): The parameters for displaying the file (size, quality, etc.).
            transform (dict): The parameters for transforming the file, add a parameter like : transforms=[
                    ["blur", 45],
                    ["tint", "rgb(255, 0, 0)"],
                    ["expand", { "right": 200, "bottom": 150 }]
        Transformations:
            fit — The fit of the thumbnail while always preserving the aspect ratio, can be any of the following options:
                cover — Covers both width/height by cropping/clipping to fit
                contain — Contain within both width/height using "letterboxing" as needed
                inside — Resize to be as large as possible, ensuring dimensions are less than or equal to the requested width and height
                outside — Resize to be as small as possible, ensuring dimensions are greater than or equal to the requested width and height
            width — The width of the thumbnail in pixels
            height — The height of the thumbnail in pixels
            quality — The optional quality of the thumbnail (1 to 100)
            withoutEnlargement — Disable image up-scaling
            format — What file format to return the thumbnail in. One of auto, jpg, png, webp, tiff
                auto — Will try to format it in webp or avif if the browser supports it, otherwise it will fallback to jpg.

        """
        if display is None:
            display = {}
        if transform is None:
            transform = []

        if len(transform) > 0:
            display["transforms"] = json.dumps(transform)

        url = f"{self.url}/assets/{file_id}?download="
        headers = self.token_header
        response = requests.get(url, headers=headers, params=display, verify=self.verify)
        if response.status_code != 200:
            raise HTTPError(response.text)
        with open(file_path, "wb") as file:
            file.write(response.content)

    def get_url_file(self, file_id: str, 
                     display: dict[str, Any] | None = None, 
                     transform: list[Any] | None = None) -> str | bytes:
        """
        Retrieve a file.

        Args:
            file_id (str): The file ID.
            **kwargs: Additional keyword arguments to pass to the request.

        Returns:
            str or bytes: The file content.
        """
        if display is None:
            display = {}
        if transform is None:
            transform = []

        url = f"{self.url}/assets/{file_id}"

        # If there are display parameters
        if transform:
            # transformer en json
            display["transforms"] = json.dumps(transform)

        # Add parameters to the URL
        if display:
            url += "?"
            url += "&".join([f"{key}={value}" for key, value in display.items()])

        return url
    
    def upload_file(self, file_path: str, data: dict[str, Any] | None = None) -> dict[str, Any]:
        """
        Upload a file.

        Args:
            file_path (str): The path to the file.
            data (dict): The file metadata (optional).

        Returns:
            dict: The uploaded file data.
        """
        if data is None:
            data = {}
        url = f"{self.url}/files"
        headers = self.token_header
        with open(file_path, 'rb') as file:
            files = {'file': file}
            response = requests.post(url, headers=headers, files=files, verify=self.verify)
        response.raise_for_status()
        
        _data = response.json()['data']
        # Mettre à jour les métadonnées du fichier
        data["type"] = guess_type(file_path)[0] or 'application/octet-stream'
        if data and response.json()['data']:
            file_id = response.json()['data']['id']
            # Mettre à jour le type du fichier
            
            
            _data = self.patch(f"/files/{file_id}", json=data)
            _data = _data['data']

        return _data

    def delete_file(self, file_id: int, **kwargs: Any):
        """
        Delete a file.

        Args:
            file_id (str): The file ID.
            **kwargs: Additional keyword arguments to pass to the request.
        """
        self.delete(f"/files/{file_id}", **kwargs)

    def get_collection(self, collection_name: str, **kwargs: Any) -> dict[str, Any]:
        """
        Get a collection.

        Args:
            collection_name (str): The collection name.
            **kwargs: Additional keyword arguments to pass to the request.

        Returns:
            dict: The collection data.
        """
        return dict(self.get(f"/collections/{collection_name}", output_type='json', **kwargs))

    def get_items(self, collection_name: str, 
                  query: dict[str, str] | None, 
                  **kwargs: Any) -> dict[str, Any]:
        """
        Get items from a collection based on the provided query.

        Args:
            collection_name (str): The collection name.
            query (dict): The search query parameters (optional).
            **kwargs: Additional keyword arguments to pass to the request.

        Returns:
            list: The list of items matching the query.
        """
        return dict(self.search(f"/items/{collection_name}", 
                           output_type='json',
                           query=query, 
                           **kwargs))

    def get_item(self, collection_name: str, item_id: int, 
                 query: dict[str, str] | None, 
                 **kwargs: Any) -> dict[str, Any]:
        """
        Get a single item from a collection based on the provided query.

        Args:
            collection_name (str): The collection name.
            item_id (str): The item ID.
            query (dict): The search query parameters (optional).
            **kwargs: Additional keyword arguments to pass to the request.

        Returns:
            dict: The item matching the query.
        """
        return dict(self.get(f"/items/{collection_name}/{item_id}", 
                        output_type='json', 
                        query=query, 
                        **kwargs))

    def create_item(self, collection_name: str, item_data: dict[str, Any], **kwargs: Any):
        """
        Create a new item in a collection.

        Args:
            collection_name (str): The collection name.
            item_data (dict): The item data.
            **kwargs: Additional keyword arguments to pass to the request.

        Returns:
            dict: The created item.
        """
        return self.post(f"/items/{collection_name}", json=item_data, **kwargs)

    def update_item(self, collection_name: str, item_id: int, item_data: dict[str, Any], **kwargs: Any):
        """
        Update an item in a collection.

        Args:
            collection_name (str): The collection name.
            item_id (str): The item ID.
            item_data (dict): The updated item data.
            **kwargs: Additional keyword arguments to pass to the request.

        Returns:
            dict: The updated item.
        """
        return self.patch(f"/items/{collection_name}/{item_id}", json=item_data, **kwargs)

    def update_file(self, item_id: int, item_data: dict[str, Any], **kwargs: Any):
        """
        Update an item in a collection.

        Args:
            collection_name (str): The collection name.
            item_id (str): The item ID.
            item_data (dict): The updated item data.
            **kwargs: Additional keyword arguments to pass to the request.

        Returns:
            dict: The updated item.
        """
        return self.patch(f"/files/{item_id}", json=item_data)
    
    def delete_item(self, collection_name: str, item_id: int, **kwargs: Any):
        """
        Delete an item from a collection.

        Args:
            collection_name (str): The collection name.
            item_id (str): The item ID.
            **kwargs: Additional keyword arguments to pass to the request.
        """
        self.delete(f"/items/{collection_name}/{item_id}", **kwargs)

    def bulk_insert(self, collection_name: str, items: list[dict[str, Any]], interval: int = 100, verbose: bool = False) -> None:
        """
        Insert multiple items into a collection in bulk.

        Args:
            collection_name (str): The collection name.
            items (list): The list of items to insert.
            interval (int): The number of items to insert per request (default: 100).
            verbose (bool): Whether to print progress information (default: False).
        """
        length = len(items)
        for i in range(0, length, interval):
            if verbose:
                print(f"Inserting {i}-{min(i + interval, length)} out of {length}")
            self.post(f"/items/{collection_name}", json=items[i:i + interval])

    def duplicate_collection(self, collection_name: str, duplicate_collection_name: str) -> None:
        """
        Duplicate a collection with its schema, fields, and data.

        Args:
            collection_name (str): The name of the collection to duplicate.
            duplicate_collection_name (str): The name of the duplicated collection.
        """
        duplicate_collection = self.get(f"/collections/{collection_name}", output_type='json')
        assert isinstance(duplicate_collection, dict) # Type narrow
        duplicate_collection['collection'] = duplicate_collection_name
        duplicate_collection['meta']['collection'] = duplicate_collection_name
        duplicate_collection['schema']['name'] = duplicate_collection_name
        self.post("/collections", json=duplicate_collection)
        
        for field in self.get_all_fields(collection_name):
            if field['schema']['is_primary_key']:
                continue
            self.post(f"/fields/{duplicate_collection_name}", json=field)

        items: list[dict[str, Any]] = self.get( # type: ignore
            f"/items/{collection_name}", 
            output_type='json', 
            params={"limit": -1}
        )
        self.bulk_insert(duplicate_collection_name, items)

    def collection_exists(self, collection_name: str) -> bool:
        """
        Check if a collection exists in Directus.

        Args:
            collection_name (str): The collection name.

        Returns:
            bool: True if the collection exists, False otherwise.
        """
        collections = self.get('/collections', output_type='json')
        assert isinstance(collections, list) # Type narrow
        return any(col['collection'] == collection_name for col in collections)

    def delete_all_items(self, collection_name: str) -> None:
        """
        Delete all items from a collection.

        Args:
            collection_name (str): The collection name.
        """
        pk_fied = self.get_pk_field(collection_name)
        if pk_fied is None:
            return
        pk_name = pk_fied['name']
        recs = self.get(f"/items/{collection_name}?fields={pk_name}", output_type='json', params={"limit": -1})
        assert isinstance(recs, list) # Type narrow
        item_ids = [data['id'] for data in recs]
        for i in range(0, len(item_ids), 100):
            self.delete(f"/items/{collection_name}", json=item_ids[i:i + 100])

    def get_all_fields(self, collection_name: str, 
                       query: dict[str, str] | None = None, 
                       **kwargs: Any) -> list[dict[str, Any]]:
        """
        Get all fields of a collection based on the provided query.

        Args:
            collection_name (str): The collection name.
            query (dict): The search query parameters (optional).
            **kwargs: Additional keyword arguments to pass to the request.

        Returns:
            list: The list of fields matching the query.
        """
        fields = self.search(f"/fields/{collection_name}", query=query, **kwargs)
        assert isinstance(fields, list) # Type narrow
        for field in fields:
            field: dict[str, Any]
            if field.get('meta') and field['meta'].get('id'):
                field['meta'].pop('id')

        return fields

    def get_pk_field(self, collection_name: str) -> dict[str, Any] | None:
        """
        Get the primary key field of a collection.

        Args:
            collection_name (str): The collection name.

        Returns:
            dict: The primary key field.
        """
        fields = self.get(f"/fields/{collection_name}", output_type='json')
        assert isinstance(fields, list) # Type narrow
        return ([field for field in fields if field['schema']['is_primary_key']] + [None]).pop()

    def get_all_user_created_collection_names(self, query: dict[str, str] | None, **kwargs: Any) -> list[Any]:
        """
        Get all user-created collection names based on the provided query.

        Args:
            query (dict): The search query parameters (optional).
            **kwargs: Additional keyword arguments to pass to the request.

        Returns:
            list: The list of user-created collection names matching the query.
        """
        collections = self.search('/collections', query=query, **kwargs)
        assert isinstance(collections, list)
        return [
            col['collection'] 
            for col in collections 
            if not col['collection'].startswith('directus')
        ]

    def get_all_fk_fields(self, collection_name: str, query: dict[str, str] | None, **kwargs: Any) -> list[Any]:
        """
        Get all foreign key fields of a collection based on the provided query.

        Args:
            collection_name (str): The collection name.
            query (dict): The search query parameters (optional).
            **kwargs: Additional keyword arguments to pass to the request.

        Returns:
            list: The list of foreign key fields matching the query.
        """
        fields = self.search(f"/fields/{collection_name}", query=query, **kwargs)
        assert isinstance(fields, list)
        return [field for field in fields if field['schema'].get('foreign_key_table')]

    def get_relations(self, collection_name: str, query: dict[str, str] | None, **kwargs: Any) -> list[Any]:
        """
        Get all relations of a collection based on the provided query.

        Args:
            collection_name (str): The collection name.
            query (dict): The search query parameters (optional).
            **kwargs: Additional keyword arguments to pass to the request.

        Returns:
            list: The list of relations matching the query.
        """
        relations = self.search(f"/relations/{collection_name}", query=query, **kwargs)
        assert isinstance(relations, list)
        return [{
            "collection": relation["collection"],
            "field": relation["field"],
            "related_collection": relation["related_collection"]
        } for relation in relations]

    def post_relation(self, relation: dict[str, Any]) -> None:
        """
        Create a new relation.

        Args:
            relation (dict): The relation data.
        """
        assert set(relation.keys()) == {'collection', 'field', 'related_collection'}
        try:
            self.post("/relations", json=relation)
        except HTTPError as e:
            if '"id" has to be unique' in str(e):
                self.post_relation(relation)
            else:
                raise
    
    def search_query(self, query: str, exclude_worlds_len: int = 2, cut_words: bool = True, **kwargs: Any) -> dict[str, Any]:
        q = []
        if cut_words:
            q = [word for word in query.split() if len(word) > exclude_worlds_len]
        else:
            q = [query]
        return {"query": {"search": q}}
