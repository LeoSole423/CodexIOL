import { el } from "./app-utils.js";
import {
  loadDashboard,
  initRangeButtons,
  initEvolutionModeControls,
  initAllocControls,
  initCashflowControls,
  initAssetPerformanceControls,
  initAssetPickerControls,
  getActiveRange,
} from "./app-dashboard.js";
import { loadQualityPage, initQualityPageControls } from "./app-quality.js";
import { loadAdvisorPage } from "./app-advisor.js";
import { loadAssetsPage } from "./app-assets.js";
import { loadHistoryPage } from "./app-history.js";

document.addEventListener("DOMContentLoaded", () => {
  initRangeButtons();
  initEvolutionModeControls();
  initAllocControls();
  initCashflowControls();
  initAssetPerformanceControls();
  initAssetPickerControls();
  if (el("chartTotal")) loadDashboard(getActiveRange());
  if (el("qualityStatusList")) {
    initQualityPageControls();
    loadQualityPage();
  }
  if (el("advisorPage")) loadAdvisorPage();
  if (el("assetsTable")) loadAssetsPage();
  if (el("chartHistory")) loadHistoryPage();
});
