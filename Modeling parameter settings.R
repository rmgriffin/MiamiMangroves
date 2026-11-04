# Useful functions
`%ni%`<- Negate(`%in%`) # Useful function

# Global settings
options(scipen = 999) # Prevent scientific notation
set.seed(1)

# Modeling parameters

# Table formatting -------------------------------------------------------
project_table <- function(x, title = NULL, note = NULL) {

  x <- x |>
    gt::tab_options(
      table.font.names = c("Arial", "Helvetica", "sans-serif"),
      table.font.size = gt::px(14),
      table.border.top.style = "solid",
      table.border.top.width = gt::px(2),
      table.border.bottom.style = "solid",
      table.border.bottom.width = gt::px(2),
      column_labels.border.bottom.style = "solid",
      column_labels.border.bottom.width = gt::px(1.5),
      data_row.padding = gt::px(8)
    )

  if (!is.null(title)) {
    x <- x |>
      gt::tab_header(title = title)
  }

  if (!is.null(note)) {
    x <- x |>
      gt::tab_source_note(
        gt::md(paste0("*Note:* ", note))
      )
  }

  x
}


