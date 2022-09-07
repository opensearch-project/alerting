[![Test Workflow](https://github.com/opensearch-project/alerting/workflows/Test%20Workflow/badge.svg)](https://github.com/opensearch-project/alerting/actions)
[![codecov](https://codecov.io/gh/opensearch-project/alerting/branch/main/graph/badge.svg)](https://codecov.io/gh/opensearch-project/alerting)
[![Documentation](https://img.shields.io/badge/api-reference-blue.svg)](https://opensearch.org/docs/latest/monitoring-plugins/alerting/api/)
[![Chat](https://img.shields.io/badge/chat-on%20forums-blue)](https://forum.opensearch.org/c/plugins/alerting/7)
![PRs welcome!](https://img.shields.io/badge/PRs-welcome!-success)

<img src="https://opensearch.org/assets/brand/SVG/Logo/opensearch_logo_default.svg" height="64px"/>

- [OpenSearch Alerting](#opensearch-alerting)
- [Highlights](#highlights)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [Code of Conduct](#code-of-conduct)
- [Security](#security)
- [License](#license)
- [Copyright](#copyright)

# OpenSearch Alerting

The OpenSearch Alerting enables you to monitor your data and send alert notifications automatically to your stakeholders. With an intuitive OpenSearch Dashboards interface and a powerful API, it is easy to set up, manage, and monitor your alerts. Craft highly specific alert conditions using Elasticsearch's full query language and scripting capabilities.


## Highlights

Scheduled searches use [cron expressions](https://en.wikipedia.org/wiki/Cron) or intervals (e.g. every five minutes) and the Elasticsearch query DSL.

To define trigger conditions, use the Painless scripting language or simple thresholds (e.g. count > 100).

When trigger conditions are met, you can publish messages to the following destinations:

* [Slack](https://slack.com/)
* Custom webhook
* [Amazon Chime](https://aws.amazon.com/chime/)
* Email

Messages can be static strings, or you can use the [Mustache](https://mustache.github.io/mustache.5.html) templates to include contextual information.


## Documentation

Please see our [documentation](https://docs-beta.opensearch.org/monitoring-plugins/alerting/index/).

## Contributing

See [developer guide](DEVELOPER_GUIDE.md) and [how to contribute to this project](CONTRIBUTING.md).

## Code of Conduct

This project has adopted the [Amazon Open Source Code of Conduct](CODE_OF_CONDUCT.md). For more information see the [Code of Conduct FAQ](https://aws.github.io/code-of-conduct-faq), or contact [opensource-codeofconduct@amazon.com](mailto:opensource-codeofconduct@amazon.com) with any additional questions or comments.

## Security

If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public GitHub issue.

## License

This project is licensed under the [Apache v2.0 License](LICENSE.txt).

## Copyright

Copyright OpenSearch Contributors. See [NOTICE](NOTICE.txt) for details.