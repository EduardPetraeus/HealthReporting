/**
 * Markdown to HTML renderer for the Health app.
 * Extracted from chat_ui.py — shared utility for all pages.
 * Supports: headers, bold, italic, tables with score colorization,
 * bullet/numbered lists, horizontal rules, paragraphs.
 */

function escapeHtml(t) {
  const d = document.createElement('div');
  d.textContent = t;
  return d.innerHTML;
}

function md(text) {
  if (!text) return '';
  let html = escapeHtml(text);

  // Horizontal rules: --- or ***
  html = html.replace(/^(-{3,}|\*{3,})$/gm, '<div class="divider"></div>');

  // Headers
  html = html.replace(/^### (.+)$/gm, '<h3>$1</h3>');
  html = html.replace(/^## (.+)$/gm, '<h2>$1</h2>');
  html = html.replace(/^# (.+)$/gm, '<h1>$1</h1>');

  // Bold
  html = html.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');

  // Italic
  html = html.replace(/(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)/g, '<em>$1</em>');

  // Tables
  html = renderTables(html);

  // Bullet lists
  html = html.replace(/^- (.+)$/gm, '<li>$1</li>');
  html = html.replace(/((?:<li>.*<\/li>\n?)+)/g, '<ul>$1</ul>');

  // Numbered lists
  html = html.replace(/^\d+\. (.+)$/gm, '<li>$1</li>');

  // Paragraphs
  html = html.replace(/\n\n+/g, '</p><p>');
  html = html.replace(/([^>])\n([^<])/g, '$1<br>$2');
  html = '<p>' + html + '</p>';

  // Clean up
  html = html.replace(/<p>\s*<\/p>/g, '');
  html = html.replace(/<p>\s*(<(?:h[123]|div|table|ul|ol)>)/g, '$1');
  html = html.replace(/(<\/(?:h[123]|div|table|ul|ol)>)\s*<\/p>/g, '$1');

  return html;
}

function renderTables(html) {
  const lines = html.split('\n');
  let result = [], tableLines = [], inTable = false;
  for (const line of lines) {
    const trimmed = line.trim();
    if (trimmed.startsWith('|') && trimmed.endsWith('|')) {
      inTable = true;
      tableLines.push(trimmed);
    } else {
      if (inTable) {
        result.push(buildTable(tableLines));
        tableLines = [];
        inTable = false;
      }
      result.push(line);
    }
  }
  if (inTable) result.push(buildTable(tableLines));
  return result.join('\n');
}

function safeCell(val) {
  const d = document.createElement('span');
  d.textContent = val;
  return d.innerHTML;
}

function buildTable(lines) {
  if (lines.length < 2) return lines.join('\n');
  const parseRow = line => line.split('|').filter((_, i, a) => i > 0 && i < a.length - 1).map(c => c.trim());
  const header = parseRow(lines[0]);
  let dataStart = 1;
  if (lines[1] && /^[\s|:-]+$/.test(lines[1].replace(/-/g, ''))) dataStart = 2;

  let t = '<table><thead><tr>';
  for (const h of header) t += '<th>' + safeCell(h) + '</th>';
  t += '</tr></thead><tbody>';
  for (let i = dataStart; i < lines.length; i++) {
    const cells = parseRow(lines[i]);
    t += '<tr>';
    for (let j = 0; j < cells.length; j++) t += '<td>' + colorizeScore(safeCell(cells[j]), header[j]) + '</td>';
    t += '</tr>';
  }
  t += '</tbody></table>';
  return '<div class="table-wrap">' + t + '</div>';
}

function colorizeScore(val, header) {
  const num = parseFloat(val);
  if (isNaN(num)) return val;
  const h = (header || '').toLowerCase();
  if (h.includes('score') || h.includes('søvn')) {
    if (num >= 80) return '<span class="score-good">' + val + '</span>';
    if (num >= 65) return '<span class="score-ok">' + val + '</span>';
    if (num > 0) return '<span class="score-low">' + val + '</span>';
  }
  if (h.includes('step') || h.includes('skridt')) {
    if (num >= 8000) return '<span class="score-good">' + val + '</span>';
    if (num >= 5000) return '<span class="score-ok">' + val + '</span>';
    if (num > 0) return '<span class="score-low">' + val + '</span>';
  }
  return val;
}
