# ----------------------------------------------
# Define API objects 
# ----------------------------------------------

# Import
import requests


class APIClient:
    """Managing the APIs"""
    def __init__(
        self, 
        auth_key : str, 
        base_url : str = "https://financialmodelingprep.com/api/v3", 
        timeout: int = 30
    ) -> None:
        
        """
        Initialize the API client with base URL and authentication details.
        
        :param base_url: The root URL for the API.
        :param auth_key: API key or authentication token. Default is FiancialModelling APIs.
        :param timeout: Request timeout in seconds.
        """
        self._auth_key = auth_key
        self.base_url = base_url
        self.timeout = timeout
    
    def get(self, endpoint : str) -> None:
        """
        Perform a GET request. 

        Attributes
        ----------
        endpoint : str 
            the endpoint of the api to add to base url for GET method. 
        
        Examples
        ---------
        ```
        # URL
        [BASE] [https://financialmodelingprep.com/api/v3] / [ENDPOINT] [symbol/available-cryptocurrencies]
        api = APIClient(auth_key, base_url)
        # Get method
        response = api.get("symbol/available-cryptocurrencies")
        r.status
        >> 200
        ```

        """
        r = requests.get(
            self.base_url + '/' + 
            endpoint + 
            f"?apikey={self._auth_key}",
            auth=("user", "pass")
        )

        return r 
