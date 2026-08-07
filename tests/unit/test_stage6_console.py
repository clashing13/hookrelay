"""Static console packaging and browser-security contracts."""

from pathlib import Path

import pytest
from fastapi import FastAPI
from httpx2 import ASGITransport, AsyncClient

from hookrelay.console import ConsoleSecurityHeadersMiddleware, mount_console


def test_console_mount_is_optional_when_assets_are_not_built(tmp_path: Path) -> None:
    app = FastAPI()

    assert mount_console(app, tmp_path) is False


@pytest.mark.asyncio
async def test_console_assets_are_same_origin_hardened_and_do_not_mask_api_404(
    tmp_path: Path,
) -> None:
    (tmp_path / "index.html").write_text("<main>HookRelay operations</main>", encoding="utf-8")
    app = FastAPI()
    app.add_middleware(ConsoleSecurityHeadersMiddleware)
    assert mount_console(app, tmp_path) is True

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        console = await client.get("/console/")
        api_missing = await client.get("/v1/does-not-exist")

    assert console.status_code == 200
    assert console.text == "<main>HookRelay operations</main>"
    assert console.headers["content-security-policy"].startswith("default-src 'self'")
    assert console.headers["x-content-type-options"] == "nosniff"
    assert console.headers["referrer-policy"] == "no-referrer"
    assert console.headers["x-frame-options"] == "DENY"
    assert api_missing.status_code == 404
    assert "HookRelay operations" not in api_missing.text
