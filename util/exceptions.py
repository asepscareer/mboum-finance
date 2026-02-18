class ScrapingError(Exception):
    """Custom exception for errors during web scraping."""
    pass

class DataNotFoundError(Exception):
    """Custom exception for when expected data is not found."""
    pass

class RequestFailedError(Exception):
    """Custom exception for when an HTTP request fails."""
    pass

class InvalidInputError(Exception):
    """Custom exception for invalid input parameters."""
    pass
