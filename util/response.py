import http

from starlette.responses import JSONResponse


def success(data):
    res = {
        "data": data,
        "message": "Success!",
        "status": http.HTTPStatus.OK,
    }

    return JSONResponse(content=res, status_code=http.HTTPStatus.OK)


def failed(msg="Make sure the input is correct!"):
    res = {
        "data": None,
        "message": msg,
        "status": http.HTTPStatus.BAD_REQUEST,
    }

    return JSONResponse(content=res, status_code=http.HTTPStatus.BAD_REQUEST)
