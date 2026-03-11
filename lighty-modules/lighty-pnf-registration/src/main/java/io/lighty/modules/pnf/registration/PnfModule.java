/*
 * Copyright (c) 2026 OPENNETS.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */

package io.lighty.modules.pnf.registration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import io.lighty.core.controller.api.AbstractLightyModule;
import io.lighty.modules.pnf.registration.common.configuration.ConfigurationFileRepresentation;
import io.lighty.modules.pnf.registration.common.configuration.filechange.IConfigChangedListener;
import io.lighty.modules.pnf.registration.mountpointregistrar.config.FaultConfig;
import io.lighty.modules.pnf.registration.mountpointregistrar.config.GeneralConfig;
import io.lighty.modules.pnf.registration.mountpointregistrar.config.MessageConfig;
import io.lighty.modules.pnf.registration.mountpointregistrar.config.PNFRegistrationConfig;
import io.lighty.modules.pnf.registration.mountpointregistrar.config.ProvisioningConfig;
import io.lighty.modules.pnf.registration.mountpointregistrar.config.StndDefinedFaultConfig;
import io.lighty.modules.pnf.registration.mountpointregistrar.config.StrimziKafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PnfModule extends AbstractLightyModule {
    private static final Logger LOG = LoggerFactory.getLogger(PnfModule.class);
    public PnfModule() {
        LOG.info("Printing from pnf module");
    }

    @Override
    @SuppressWarnings({"checkstyle:illegalCatch"})
    protected boolean initProcedure() {
        LOG.info("initProcedure PNF Registration module");
        LOG.info("Init call for {}", APPLICATION_NAME);

        ConfigurationFileRepresentation configFileRepresentation =
                new ConfigurationFileRepresentation(CONFIGURATIONFILE);
        configFileRepresentation.registerConfigChangedListener(this);

        generalConfig = new GeneralConfig(configFileRepresentation);
        strimziKafkaConfig = new StrimziKafkaConfig(configFileRepresentation);
        PNFRegistrationConfig pnfRegConfig = new PNFRegistrationConfig(configFileRepresentation);
        FaultConfig faultConfig = new FaultConfig(configFileRepresentation);
        ProvisioningConfig provisioningConfig = new ProvisioningConfig(configFileRepresentation);
        StndDefinedFaultConfig stndFaultConfig = new StndDefinedFaultConfig(configFileRepresentation);

        configMap.put("pnfRegistration", pnfRegConfig);
        configMap.put("fault", faultConfig);
        configMap.put("provisioning", provisioningConfig);
        configMap.put("stndDefinedFault", stndFaultConfig);

        strimziEnabled = strimziKafkaConfig.getEnabled();
        if (strimziEnabled) { // start Kafka consumer thread only if strimziEnabled=true
            LOG.info("Strimzi Kafka seems to be enabled, starting consumer(s)");
            sKafkaConsumerMain = new StrimziKafkaVESMsgConsumerMain(configMap, generalConfig, strimziKafkaConfig);
            sKafkaVESMsgConsumerMain = new Thread(sKafkaConsumerMain);
            sKafkaVESMsgConsumerMain.start();
        } else {
            LOG.info("Strimzi Kafka seems to be disabled, not starting any consumer(s)");
        }
        return true;
    }

    @SuppressWarnings("checkstyle:illegalCatch")
    @Override
    protected boolean stopProcedure() {
        LOG.info("Stopping PNF Registration module");
        // cleanup logic here
        return true;
    }

    @SuppressWarnings("unused")
    private void close(AutoCloseable... toCloseList) throws Exception {
        for (AutoCloseable element : toCloseList) {
            if (element != null) {
                element.close();
            }
        }
    }
}