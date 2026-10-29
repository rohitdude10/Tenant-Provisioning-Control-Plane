import asyncio

import httpx

from app.main import app


def test_request_validation_uses_structured_error_response():
    async def request():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.post(
                "/tenants",
                json={"slug": "bad_slug", "name": "Invalid"},
            )

    response = asyncio.run(request())

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"
    assert response.json()["error"]["details"]


def test_unknown_route_uses_structured_error_response():
    async def request():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.get("/not-a-route")

    response = asyncio.run(request())

    assert response.status_code == 404
    assert response.json() == {"error": {"code": "http_error", "message": "Not Found"}}
