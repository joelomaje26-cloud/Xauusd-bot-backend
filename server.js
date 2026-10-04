const APP_ID = process.env.DERIV_APP_ID; // alphanumeric, not 1089
const TOKEN = process.env.DERIV_TOKEN; // pat_...

async function getAuthenticatedWsUrl() {
  const headers = {
    Authorization: `Bearer ${TOKEN}`,
    "Deriv-App-ID": APP_ID,
  };
  const accRes = await fetch("https://api.derivws.com/trading/v1/options/accounts", { headers });
  if (!accRes.ok) throw new Error(`Accounts failed: ${accRes.status} ${await accRes.text()}`);
  const accJson = await accRes.json();
  const accounts = accJson.data || accJson.accounts || [];
  const account = accounts.find(a => a.account_type === "demo" || a.group === "demo") || accounts[0];
  if (!account) throw new Error("No Deriv accounts found");

  const otpRes = await fetch(`https://api.derivws.com/trading/v1/options/accounts/${account.account_id}/otp`, {
    method: "POST", headers
  });
  if (!otpRes.ok) throw new Error(`OTP failed: ${otpRes.status} ${await otpRes.text()}`);
  const otpJson = await otpRes.json();
  return otpJson.data.url;
}

async function connectDeriv() {
  try {
    const url = await getAuthenticatedWsUrl();
    console.log("Got OTP WS URL, connecting...");
    const ws = new WebSocket(url);
    ws.on("open", () => {
      console.log("Deriv WS connected");
      // Send proposal directly, no authorize
      ws.send(JSON.stringify({
        proposal: 1, amount: 1, basis: "stake",
        contract_type: "MULTUP", currency: "USD",
        duration_unit: "s", multiplier: 40,
        underlying_symbol: "1HZ100V",
      }));
    });
    ws.on("message", d => console.log("Deriv:", d.toString()));
    ws.on("close", () => setTimeout(connectDeriv, 5000));
  } catch (e) {
    console.error("Deriv connect failed:", e.message);
    setTimeout(connectDeriv, 5000);
  }
}
