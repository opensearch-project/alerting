/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.alerting.transport

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope

/**
 * A test that keep a singleton node started for all tests that can be used to get
 * references to Guice injectors in unit tests.
 */

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
abstract class WorkflowSingleNodeTestCase : AlertingSingleNodeTestCase()
