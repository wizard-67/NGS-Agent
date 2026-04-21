#!/usr/bin/env Rscript
suppressPackageStartupMessages({
  library(DESeq2)
  library(ggplot2)
  library(dplyr)
  library(tibble)
  library(readr)
  library(jsonlite)
  library(matrixStats)
  library(ComplexHeatmap)
  library(circlize)
  library(EnhancedVolcano)
  library(plotly)
  library(htmlwidgets)
})

args <- commandArgs(trailingOnly = TRUE)
count_path <- args[1]
sample_sheet_path <- args[2]
out_dir <- args[3]

dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

counts <- read_csv(count_path, show_col_types = FALSE)
samples <- read_csv(sample_sheet_path, show_col_types = FALSE)

if (!"sample_id" %in% colnames(samples) || !"condition" %in% colnames(samples)) {
  stop("sample_sheet must include sample_id and condition columns")
}

counts_df <- as.data.frame(counts)
rownames(counts_df) <- counts_df[[1]]
counts_df[[1]] <- NULL
count_mat <- as.matrix(counts_df)

sample_ids <- samples$sample_id
common <- intersect(colnames(count_mat), sample_ids)
if (length(common) < 2) stop("Count matrix and sample sheet do not overlap enough for DESeq2")
count_mat <- count_mat[, common, drop = FALSE]
samples <- samples[match(common, samples$sample_id), , drop = FALSE]
rownames(samples) <- samples$sample_id

if (!"batch" %in% colnames(samples)) samples$batch <- "batch1"
if (length(unique(samples$condition)) < 2) stop("Need at least two conditions for DESeq2")

dds <- DESeqDataSetFromMatrix(
  countData = round(count_mat),
  colData = samples,
  design = ~ batch + condition
)
dds <- DESeq(dds)
res <- results(dds, contrast = c("condition", levels(factor(samples$condition))[2], levels(factor(samples$condition))[1]))
res_df <- as.data.frame(res) %>% rownames_to_column("gene") %>% arrange(padj)
write_csv(res_df, file.path(out_dir, "deseq_results.csv"))

vsd <- vst(dds, blind = FALSE)
pca_data <- plotPCA(vsd, intgroup = c("condition"), returnData = TRUE)
percentVar <- round(100 * attr(pca_data, "percentVar"))

pca_plot <- ggplot(pca_data, aes(PC1, PC2, color = condition)) +
  geom_point(size = 4) +
  theme_minimal(base_size = 14) +
  labs(
    title = "PCA of RNA-Seq Samples",
    subtitle = ifelse(percentVar[1] < 50, "Warning: PC1 explains <50% variance", NULL)
  )

ggsave(file.path(out_dir, "pca.png"), pca_plot, width = 8, height = 6, dpi = 300)
ggsave(file.path(out_dir, "pca.pdf"), pca_plot, width = 8, height = 6)

sig <- !is.na(res_df$padj) & res_df$padj < 0.05
ma_plot <- ggplot(res_df, aes(x = baseMean, y = log2FoldChange, text = gene)) +
  geom_point(aes(color = sig), alpha = 0.75) +
  scale_x_log10() +
  theme_minimal(base_size = 14) +
  labs(title = "MA Plot", x = "Base Mean (log10)", y = "Log2 Fold Change")

ma_html <- ggplotly(ma_plot, tooltip = c("text", "x", "y"))
saveWidget(ma_html, file.path(out_dir, "ma_plot.html"), selfcontained = TRUE)

top10 <- res_df %>% filter(!is.na(padj)) %>% arrange(padj) %>% slice_head(n = 10) %>% pull(gene)
volcano <- EnhancedVolcano(
  res_df,
  lab = res_df$gene,
  x = "log2FoldChange",
  y = "padj",
  pCutoff = 0.05,
  FCcutoff = 1,
  selectLab = top10,
  drawConnectors = TRUE,
  title = "Volcano Plot"
)

ggsave(file.path(out_dir, "volcano.png"), volcano, width = 8, height = 7, dpi = 300)

mat <- assay(vsd)
vars <- rowVars(mat)
top_var <- head(order(vars, decreasing = TRUE), 50)
hm <- mat[top_var, , drop = FALSE]
hm_z <- t(scale(t(hm)))

png(file.path(out_dir, "heatmap.png"), width = 2400, height = 2000, res = 300)
Heatmap(
  hm_z,
  name = "Z-score",
  cluster_rows = TRUE,
  cluster_columns = TRUE,
  show_row_names = FALSE,
  top_annotation = HeatmapAnnotation(df = samples[, c("condition"), drop = FALSE])
)
dev.off()

warnings <- c()
if (percentVar[1] < 50) warnings <- c(warnings, "PC1 variance below 50%")

summary <- list(
  n_genes_tested = nrow(res_df),
  n_sig = sum(!is.na(res_df$padj) & res_df$padj < 0.05),
  pc1_variance = percentVar[1],
  warnings = warnings
)
write_json(summary, file.path(out_dir, "de_summary.json"), auto_unbox = TRUE, pretty = TRUE)
writeLines(c(
  "DESeq2 analysis completed successfully.",
  paste0("Significant genes: ", summary$n_sig),
  paste0("PC1 variance: ", summary$pc1_variance, "%")
), file.path(out_dir, "report_notes.md"))
