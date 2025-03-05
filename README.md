# pg-flake

Analyze the errors for the day, group the logs to find which ones are related, then search the commits to find
possible culprits.

The challenge is that many errors may not be net-new, so trying the explanation for them in the day's logs
is a fool's errand. So we start by looking for evidence of an anomalous new error