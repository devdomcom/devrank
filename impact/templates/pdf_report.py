from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle


def generate_candidate_pdf(metrics_results, user_login, period_str, output_path="candidate_report.pdf"):
    """Modern/professional PDF (clean blues/grays, centered subtitle, no overflow, formatted details)."""
    doc = SimpleDocTemplate(output_path, pagesize=letter, leftMargin=0.75*inch, rightMargin=0.75*inch, topMargin=0.75*inch)
    styles = getSampleStyleSheet()
    # Modern/professional styles (sans, colors, spacing; margins fixed)
    title_style = ParagraphStyle("Title", parent=styles["Title"], fontName="Helvetica-Bold", fontSize=28, textColor=colors.HexColor("#1E3A8A"), alignment=1, spaceAfter=12)
    subtitle_style = ParagraphStyle("Subtitle", parent=styles["Normal"], fontName="Helvetica", fontSize=16, textColor=colors.HexColor("#64748B"), alignment=1, spaceBefore=6, spaceAfter=30)
    section_style = ParagraphStyle("Section", parent=styles["Heading2"], fontName="Helvetica-Bold", fontSize=18, textColor=colors.HexColor("#334155"), spaceAfter=12, spaceBefore=18)
    rating_style = ParagraphStyle("Rating", parent=styles["Heading3"], fontName="Helvetica-Bold", fontSize=16, textColor=colors.HexColor("#3B82F6"), spaceAfter=12, leftIndent=12)
    summary_style = ParagraphStyle("Summary", parent=styles["Normal"], fontSize=12, textColor=colors.HexColor("#475569"), leftIndent=12, spaceAfter=12)
    detail_style = ParagraphStyle("Detail", parent=styles["Normal"], fontSize=10, leftIndent=12, spaceAfter=6)

    elements = []
    # Header (centered title/subtitle)
    elements.append(Paragraph(f"Candidate: {user_login}", title_style))
    elements.append(Paragraph(f"Analysis Period: {period_str}", subtitle_style))
    elements.append(Spacer(1, 0.4 * inch))

    for result in metrics_results:
        # Metric section
        elements.append(Paragraph(f"{result['name']} ({result['slug']})", section_style))
        elements.append(Paragraph(f"Rating: <b>{result['rating'].upper()}</b>", rating_style))
        elements.append(Paragraph(result.get("description", ""), detail_style))
        elements.append(Paragraph(f"<i>Summary: {result['summary']}</i>", summary_style))
        # Details (unpack lists/objects to bullets/Paragraphs; no overflow, consistent padding)
        if result.get("details"):
            elements.append(Paragraph("<b>Details:</b>", detail_style))
            for k, v in result["details"].items():
                if isinstance(v, list):
                    elements.append(Paragraph(f"<b>{k}:</b> {len(v)} items", detail_style))
                    for item in v[:5]:  # unpack sample
                        elements.append(Paragraph(f"  • {str(item)[:120]}", detail_style))
                else:
                    elements.append(Paragraph(f"<b>{k}:</b> {str(v)}", detail_style))
        elements.append(Spacer(1, 0.25 * inch))

    doc.build(elements)
    print(f"PDF exported to {output_path} (modern layout applied)")
