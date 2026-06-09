# @osvaldoandrade/cs

`cs` is the command-line client for [code-sous](https://github.com/osvaldoandrade/sous), a serverless functions runtime.

This npm package distributes the prebuilt Go binary; `npm install` downloads it from the matching GitHub Release.

## Install

```sh
npm install -g @osvaldoandrade/cs
cs --help
```

## Upgrade

```sh
npm install -g @osvaldoandrade/cs@latest
```

## Authenticate against a cluster

```sh
cs auth login --tenant t_abc123 --token "$CS_TOKEN" --api-url https://sous.example.com
```

## Author and publish a function

```sh
cs fn init reconcile --runtime cs-js
cs fn test reconcile --event ./event.json
cs fn draft upload reconcile --path .
cs fn publish reconcile --draft <draft_id> --memory-mb 64 \
  --invoke-http-roles role:app
cs fn alias set reconcile prod --version 1
```

## Documentation

- Full docs: https://osvaldoandrade.github.io/sous/wiki
- Source repo: https://github.com/osvaldoandrade/sous
- Issues: https://github.com/osvaldoandrade/sous/issues

## Notes

- If the source repository is private, set `GITHUB_TOKEN` or `GH_TOKEN` in the environment so the postinstall step can authenticate the binary download.
