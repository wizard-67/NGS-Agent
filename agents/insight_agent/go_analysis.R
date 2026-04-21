#!/usr/bin/env Rscript
suppressPackageStartupMessages({
  library(clusterProfiler)
  library(org.Hs.eg.db)
  library(readr)
  library(dplyr)
  library(tibble)
  library(jsonlite)
})

args <- commandArgs(trailingOnly = TRUE)
de_path <- args[1]
go_input_path <- args[2]
out_dir <- args[3]

dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

de <- read_csv(de_path, show_col_types = FALSE)
go_input <- read_csv(go_input_path, show_col_types = FALSE)

if (!"gene" %in% colnames(de)) stop("DE results must include gene column")
if (!"gene" %in% colnames(go_input)) stop("GO input must include gene column")

sig <- de %>% filter(!is.na(padj), padj < 0.05, abs(log2FoldChange) > 1)

genes <- unique(sig$gene)
if (length(genes) == 0) {
  write_csv(tibble(ID = character(), Description = character(), p.adjust = numeric()), file.path(out_dir, "go_enrichment.csv"))
  quit(save = "no")
}

# This expects SYMBOL input; adapt ID type upstream if needed.
entrez <- bitr(genes, fromType = "SYMBOL", toType = "ENTREZID", OrgDb = org.Hs.eg.db)
if (nrow(entrez) == 0) {
  write_csv(tibble(ID = character(), Description = character(), p.adjust = numeric()), file.path(out_dir, "go_enrichment.csv"))
  quit(save = "no")
}

ego <- enrichGO(
  gene = unique(entrez$ENTREZID),
  OrgDb = org.Hs.eg.db,
  keyType = "ENTREZID",
  ont = "BP",
  pAdjustMethod = "BH",
  readable = TRUE
)

go_df <- as.data.frame(ego)
write_csv(go_df, file.path(out_dir, "go_enrichment.csv"))
write_json(list(n_sig = nrow(sig), n_go = nrow(go_df)), file.path(out_dir, "go_summary.json"), auto_unbox = TRUE, pretty = TRUE)
