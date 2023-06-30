## Guidance for Automated Restore and Copy for Amazon S3 Glacier Objects

## Description
Amazon S3 Automated Archive Restore and Copy solution provides an automated workflow to restore archived S3 data stored in Glacier Flexible Retrieval and Deep Archive class to a new storage class, using an in-place copy, copy to a new prefix within the same S3 bucket or to another Amazon S3 bucket in the same or different AWS Account or Region. The solution orchestrates the steps involved in Archive restore, including Amazon S3 bucket Inventory generation, manifest query and optimization, archive retrieval, and copy process. It also tracks the progress and send job status notifications to the requester.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

