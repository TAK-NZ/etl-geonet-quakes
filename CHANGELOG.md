# CHANGELOG

## Emoji Cheatsheet
- :pencil2: doc updates
- :bug: when fixing a bug
- :rocket: when making general improvements
- :white_check_mark: when adding tests
- :arrow_up: when upgrading dependencies
- :tada: when adding new features

## Version History

### v1.0.0

- :tada: Initial Commit
### v1.2.0
- :tada: Add a `capabilities.json` manifest so CloudTAK can read the task's requirements from the image. It declares a single required permission, `feature:submit` (the only CloudTAK API the task uses is `submit()`), 1024 MB memory / 120 s timeout, and a default `rate(2 minutes)` schedule. The schedule and compute values are judgement calls (nothing in the repo specifies them; features go stale after 5 minutes). The manifest is validated against `StaticCapabilitiesSchema` from `@tak-ps/etl` by a test
- :rocket: Build and push the image with `docker buildx` in the demo and production deploy jobs, embedding `capabilities.json` as the `com.cloudtak.capabilities` OCI annotation, with `docker/setup-buildx-action@v4` providing the `docker-container` builder the annotation needs. This only runs on version tags / manual dispatch, so it has not been exercised in CI or in the demo environment
- :rocket: Deliberately NOT adopting the `cloudtak-etl` CLI from `@tak-ps/etl` for the build and push: it hardcodes the destination ECR repository as `tak-vpc-<Environment>-cloudtak-tasks`, which does not match the `<stackname>-etltasks` repository used by TAK.NZ base-infra. The existing lookup through the `EcrEtlTasksRepoArn` CloudFormation export is kept unchanged
- :white_check_mark: Add a basic test suite (`npm test`, `node:test` run through `tsx`) covering the task's static config, input and output schemas and the manifest; the `lint` script now also covers `test/`
- :rocket: Use `Task.init()` for the local and Lambda entry points. No change in Lambda behaviour, `ETL_TOKEN` is always provided there
- :rocket: Require Node 24 (`engines` `>= 24`), and use Node 24 in the lint and deploy workflows (they were still on Node 18), matching the Lambda base image
- :arrow_up: Update dependencies within their existing ranges (`npm update`, no `overrides`): `@tak-ps/etl` 10.22.2, `eslint` 10.12.0 and `typescript-eslint` 8.71.1, and raise the `@tak-ps/etl` minimum to `^10.13.0` (needed for the capabilities schema). Add `tsx` ^4.23.15 as a dev dependency. `npm audit` now reports 0 vulnerabilities (7 before: 1 critical, 3 high, 3 moderate). `typescript` stays on 6.0.3 as `typescript-eslint` still limits supported versions to below 6.1.0
- :rocket: Add a `.dockerignore` so `.git`, `.github`, `node_modules`, `dist`, `test`, `docs`, `.agents`, `.env*` and markdown files are kept out of the image build context. `capabilities.json`, `task.ts`, `package*.json` and `tsconfig.json` stay in the context
- :pencil2: The `license` field in `package.json` (`AGPL-3.0-only`) was checked against the `LICENSE` file (GNU AGPL v3) and already matches; no change
- :pencil2: `package.json` was still at 1.1.2 although tags go up to v1.1.6; this release sets it to 1.2.0
- :arrow_up: Update GitHub Actions to releases that run on Node.js 24, clearing the Node.js 20 deprecation warnings: `actions/checkout` v7, `actions/setup-node` v7 and `aws-actions/configure-aws-credentials` v6. `aws-actions/amazon-ecr-login` v2 already runs on Node.js 24. Not yet run in CI on these versions
- :rocket: Pin the workflow runners to `ubuntu-24.04` instead of `ubuntu-latest`, so the `ubuntu-latest` migration to Ubuntu 26 (starting October 19, 2026) does not change the build environment unannounced
