from typing import Any, Optional
from pydantic import BaseModel
from starlette.responses import JSONResponse
import http

class BaseResponse(BaseModel):
    status_code: int
    message: str

class SuccessResponse(BaseResponse):
    data: Any

class ErrorResponse(BaseResponse):
    data: Optional[Any] = None
    trace_id: Optional[str] = None # Added trace_id

def success(data: Any, message: str = "Success!") -> JSONResponse:
    response_content = SuccessResponse(
        status_code=http.HTTPStatus.OK,
        message=message,
        data=data
    ).model_dump()
    return JSONResponse(content=response_content, status_code=http.HTTPStatus.OK)

def failed(message: str = "Make sure the input is correct!", status_code: int = http.HTTPStatus.BAD_REQUEST, trace_id: Optional[str] = None) -> JSONResponse:
    response_content = ErrorResponse(
        status_code=status_code,
        message=message,
        data=None,
        trace_id=trace_id # Pass trace_id to the model
    ).model_dump()
    return JSONResponse(content=response_content, status_code=status_code)
