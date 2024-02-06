import json

import httpx


class RequestError(Exception):
    def __init__(self, message, status):
        super().__init__(message)
        self.status = status


class AnalyticsClient:
    def __init__(self):
        self.api_base_url = "https://appstoreconnect.apple.com/analytics/api/v1"
        self.default_headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/javascript, */*",
        }
        self.cookies = {}

    def set_cookies(self, response):
        set_cookie_header = response.headers.get("set-cookie")
        if set_cookie_header:
            cookie_pairs = set_cookie_header.split(";")
            for pair in cookie_pairs:
                key, value = pair.split("=")
                self.cookies[key] = value

    @property
    def headers(self):
        cookies = " ".join([f"{k}={v}" for k, v in self.cookies.items()])
        return {**self.default_headers, "Cookie": cookies}

    @staticmethod
    def check_response_for_error(response, start_message, end_message=None):
        if not response.ok:
            raise RequestError(
                f"{start_message}: {response.status} {response.statusText} {end_message or ''}",  # noqa
                response.status,
            )

    async def login(self, username, password, test_code=None):
        base_auth_url = "https://idmsa.apple.com/appleauth/auth"
        session_url = "https://appstoreconnect.apple.com/olympus/v1/session"
        login_headers = {
            "X-Apple-Widget-Key": "e0b80c3bf78523bfe80974d320935bfa30add02e1bff88ec2166c6bd5a706c42",  # noqa
        }

        login_response = await httpx.post(
            f"{base_auth_url}/signin?isRememberMeEnabled=true",
            json={
                "accountName": username,
                "password": password,
                "rememberMe": False,
            },
            headers={**self.headers, **login_headers},
        )

        if not login_response.ok:
            if login_response.status == 409:
                print("Attempting to handle 2-step verification")
                login_headers["X-Apple-ID-Session-Id"] = login_response.headers.get(
                    "X-Apple-ID-Session-Id"
                )
                login_headers["scnt"] = login_response.headers.get("scnt")
                code_request_response = await httpx.get(
                    base_auth_url, headers={**self.headers, **login_headers}
                )

                if not code_request_response.ok:
                    if code_request_response.status == 423:
                        print(
                            "Too many codes requested, try again later or use last code"
                        )
                    else:
                        self.check_response_for_error(
                            code_request_response, "Error requesting 2SV code"
                        )

                if test_code:
                    code = test_code
                else:
                    code = input("Enter 2SV code: ")
                    if not code:
                        raise ValueError("No 2SV code given")

                login_response = await httpx.post(
                    f"{base_auth_url}/verify/phone/securitycode",
                    json={
                        "mode": "sms",
                        "phoneNumber": {"id": 1},
                        "securityCode": {"code": code},
                    },
                    headers={**self.headers, **login_headers},
                )
            elif login_response.status == 412:
                login_headers["X-Apple-ID-Session-Id"] = login_response.headers.get(
                    "X-Apple-ID-Session-Id"
                )
                login_headers["scnt"] = login_response.headers.get("scnt")
                login_response = await httpx.post(
                    f"{base_auth_url}/repair/complete",
                    headers={**self.headers, **login_headers},
                )

                self.check_response_for_error(
                    login_response, "Error skipping 2SV request"
                )
            else:
                message = (
                    "Invalid username and password"
                    if login_response.status == 401
                    else "Unrecognized error"
                )
                self.check_response_for_error(
                    login_response, "Could not log in", message
                )

        self.set_cookies(login_response)
        if "myacinfo" not in self.cookies:
            raise ValueError("Could not find account info cookie")

        session_response = await httpx.get(session_url, headers=self.headers)
        self.check_response_for_error(session_response, "Could not get session cookie")

        self.set_cookies(session_response)
        if "itctx" not in self.cookies:
            raise ValueError("Could not find session cookie")

    def is_authenticated(self, name):
        if "myacinfo" not in self.cookies or "itctx" not in self.cookies:
            raise ValueError(
                f"{name} function requires authentication; use login function first"
            )

    async def get_metadata(self):
        # self.is_authenticated("get_metadata")

        settings_response = await httpx.get(
            f"{self.api_base_url}/settings/all", headers=self.headers
        )

        data = json.loads(await settings_response.text())
        self.check_response_for_error(
            settings_response, "Could not get API settings", data.get("errors")
        )

        return data

    async def get_metric(self, app_id, metric, dimension, start_date, end_date):
        # self.is_authenticated("get_metric")

        request_body = {
            "adamId": [app_id],
            "measures": [metric] if not isinstance(metric, list) else metric,
            "group": {
                "dimension": dimension,
                "metric": metric,
                "limit": 10,
                "rank": "DESCENDING",
            }
            if dimension
            else None,
            "frequency": "day",
            "startTime": f"{start_date}T00:00:00Z",
            "endTime": f"{end_date}T00:00:00Z",
        }

        metrics_response = await httpx.post(
            "https://appstoreconnect.apple.com/analytics/api/v1/data/time-series",
            json=request_body,
            headers={**self.headers, "X-Requested-By": "dev.apple.com"},
        )

        data = json.loads(await metrics_response.text())
        self.check_response_for_error(
            metrics_response,
            "Could not get metrics",
            f"\n{json.dumps(data.get('errors'), indent=2) or ''}",
        )

        return data
