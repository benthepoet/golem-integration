import { GolemNetwork } from "@golem-sdk/golem-js";
import { pinoPrettyLogger } from "@golem-sdk/pino-logger";
import config from "config";

// Initialize Golem Network client
const glm = new GolemNetwork({
  logger: pinoPrettyLogger({ level: "debug" }),
  api: {
    key: config.get("apiKey")
  },
  payment: {
    network: config.get("paymentNetwork")
  }
});

export default glm;
