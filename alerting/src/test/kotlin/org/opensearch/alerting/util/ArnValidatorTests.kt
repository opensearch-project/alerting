/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.util

import org.opensearch.OpenSearchStatusException
import org.opensearch.commons.alerting.model.Target
import org.opensearch.test.OpenSearchTestCase

class ArnValidatorTests : OpenSearchTestCase() {

    fun `test valid AOS domain ARN passes validation`() {
        val target = Target(
            "AOS_DOMAIN",
            "https://my-domain.us-west-2.es.amazonaws.com",
            "arn:aws:es:us-west-2:123456789012:domain/my-domain"
        )
        ArnValidator.validateTargetArn(target)
    }

    fun `test valid ARN with govcloud partition passes validation`() {
        val target = Target(
            "AOS_DOMAIN",
            "https://my-domain.us-gov-west-1.es.amazonaws.com",
            "arn:aws-us-gov:es:us-gov-west-1:123456789012:domain/my-domain"
        )
        ArnValidator.validateTargetArn(target)
    }

    fun `test null target passes validation`() {
        ArnValidator.validateTargetArn(null)
    }

    fun `test LOCAL target skips validation`() {
        val target = Target(Target.LOCAL, "", "")
        ArnValidator.validateTargetArn(target)
    }

    fun `test blank arn for non-LOCAL target throws at construction`() {
        // Target init block rejects blank/whitespace arn for non-LOCAL types
        expectThrows(IllegalArgumentException::class.java) {
            Target("AOS_DOMAIN", "https://my-domain.us-west-2.es.amazonaws.com", "")
        }
    }

    fun `test malformed arn throws`() {
        val target = Target("AOS_DOMAIN", "https://my-domain.us-west-2.es.amazonaws.com", "not-an-arn")
        expectThrows(OpenSearchStatusException::class.java) {
            ArnValidator.validateTargetArn(target)
        }
    }

    fun `test arn missing account ID throws`() {
        val target = Target("AOS_DOMAIN", "https://my-domain.us-west-2.es.amazonaws.com", "arn:aws:es:us-west-2::domain/my-domain")
        expectThrows(OpenSearchStatusException::class.java) {
            ArnValidator.validateTargetArn(target)
        }
    }

    fun `test arn missing resource throws`() {
        val target = Target("AOS_DOMAIN", "https://my-domain.us-west-2.es.amazonaws.com", "arn:aws:es:us-west-2:123456789012:")
        expectThrows(OpenSearchStatusException::class.java) {
            ArnValidator.validateTargetArn(target)
        }
    }
}
