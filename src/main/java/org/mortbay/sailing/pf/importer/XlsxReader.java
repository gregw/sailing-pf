package org.mortbay.sailing.pf.importer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Reads the cached cell values of an .xlsx workbook using only the JDK (the file is a zip
 * of SpreadsheetML parts). Formulas are not evaluated: each cell yields the value Excel
 * last saved for it, as text. Numbers keep Excel's representation, so dates and times are
 * day fractions; see {@link #toDate}.
 */
public final class XlsxReader
{
    private static final String MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
    private static final String REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";

    /** A workbook: its sheets in workbook order, keyed by sheet name. */
    public record Workbook(Map<String, Sheet> sheets, boolean date1904)
    {
        public Sheet sheet(String name)
        {
            return sheets.get(name);
        }

        /** The date for an Excel day number in this workbook's date system; null if not a number. */
        public LocalDate toDate(String value)
        {
            return XlsxReader.toDate(value, date1904);
        }
    }

    /** A sheet: its non-empty rows, each a list of cell texts ("" for an empty cell). */
    public record Sheet(String name, List<List<String>> rows) {}

    private XlsxReader() {}

    public static Workbook read(byte[] xlsx) throws IOException
    {
        Map<String, byte[]> parts = new HashMap<>();
        try (ZipInputStream zin = new ZipInputStream(new ByteArrayInputStream(xlsx)))
        {
            ZipEntry e;
            while ((e = zin.getNextEntry()) != null)
            {
                if (!e.isDirectory())
                    parts.put(e.getName(), zin.readAllBytes());
            }
        }
        try
        {
            DocumentBuilder db = documentBuilder();
            List<String> shared = sharedStrings(db, parts.get("xl/sharedStrings.xml"));
            Document workbook = parse(db, required(parts, "xl/workbook.xml"));
            Map<String, String> rels = relationships(db, required(parts, "xl/_rels/workbook.xml.rels"));

            boolean date1904 = false;
            NodeList pr = workbook.getElementsByTagNameNS(MAIN_NS, "workbookPr");
            if (pr.getLength() > 0)
            {
                String d = ((Element)pr.item(0)).getAttribute("date1904");
                date1904 = "1".equals(d) || "true".equalsIgnoreCase(d);
            }

            Map<String, Sheet> sheets = new LinkedHashMap<>();
            NodeList sheetNodes = workbook.getElementsByTagNameNS(MAIN_NS, "sheet");
            for (int i = 0; i < sheetNodes.getLength(); i++)
            {
                Element s = (Element)sheetNodes.item(i);
                String name = s.getAttribute("name");
                String target = rels.get(s.getAttributeNS(REL_NS, "id"));
                if (target == null)
                    continue;
                target = target.startsWith("/") ? target.substring(1) : "xl/" + target;
                byte[] xml = parts.get(target);
                if (xml != null)
                    sheets.put(name, new Sheet(name, rows(parse(db, xml), shared)));
            }
            return new Workbook(sheets, date1904);
        }
        catch (IOException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            throw new IOException("Unreadable xlsx: " + e.getMessage(), e);
        }
    }

    /** The date for an Excel day number (1900 or 1904 date system); null if not a number. */
    public static LocalDate toDate(String value, boolean date1904)
    {
        if (value == null || value.isBlank())
            return null;
        try
        {
            long days = (long)Math.floor(Double.parseDouble(value));
            return date1904 ? LocalDate.of(1904, 1, 1).plusDays(days)
                : LocalDate.of(1899, 12, 30).plusDays(days);
        }
        catch (NumberFormatException e)
        {
            return null;
        }
    }

    private static List<List<String>> rows(Document sheet, List<String> shared)
    {
        List<List<String>> rows = new ArrayList<>();
        NodeList rowNodes = sheet.getElementsByTagNameNS(MAIN_NS, "row");
        for (int r = 0; r < rowNodes.getLength(); r++)
        {
            Map<Integer, String> cells = new HashMap<>();
            int max = -1;
            for (Node n = rowNodes.item(r).getFirstChild(); n != null; n = n.getNextSibling())
            {
                if (!(n instanceof Element c) || !"c".equals(c.getLocalName()))
                    continue;
                String text = cellText(c, shared);
                if (text == null || text.isBlank())
                    continue;
                int col = columnIndex(c.getAttribute("r"));
                if (col < 0)
                    continue;
                cells.put(col, text.trim());
                max = Math.max(max, col);
            }
            if (max < 0)
                continue;
            List<String> row = new ArrayList<>(max + 1);
            for (int i = 0; i <= max; i++)
                row.add(cells.getOrDefault(i, ""));
            rows.add(row);
        }
        return rows;
    }

    private static String cellText(Element c, List<String> shared)
    {
        String type = c.getAttribute("t");
        if ("inlineStr".equals(type))
            return allText(c);
        String v = childText(c, "v");
        if (v == null)
            return null;
        if ("s".equals(type))
        {
            try
            {
                int i = Integer.parseInt(v.trim());
                return i >= 0 && i < shared.size() ? shared.get(i) : null;
            }
            catch (NumberFormatException e)
            {
                return null;
            }
        }
        return v;
    }

    /** "B12" → 1; -1 if the reference has no column letters. */
    static int columnIndex(String ref)
    {
        int n = 0;
        int i = 0;
        while (i < ref.length() && Character.isLetter(ref.charAt(i)))
        {
            n = n * 26 + (Character.toUpperCase(ref.charAt(i)) - 'A' + 1);
            i++;
        }
        return i == 0 ? -1 : n - 1;
    }

    private static List<String> sharedStrings(DocumentBuilder db, byte[] xml) throws Exception
    {
        List<String> strings = new ArrayList<>();
        if (xml == null)
            return strings;
        NodeList items = parse(db, xml).getElementsByTagNameNS(MAIN_NS, "si");
        for (int i = 0; i < items.getLength(); i++)
            strings.add(allText((Element)items.item(i)));
        return strings;
    }

    private static Map<String, String> relationships(DocumentBuilder db, byte[] xml) throws Exception
    {
        Map<String, String> rels = new HashMap<>();
        NodeList nodes = parse(db, xml).getElementsByTagName("Relationship");
        for (int i = 0; i < nodes.getLength(); i++)
        {
            Element r = (Element)nodes.item(i);
            rels.put(r.getAttribute("Id"), r.getAttribute("Target"));
        }
        return rels;
    }

    /** Concatenated text of every {@code <t>} under the element (rich text runs included). */
    private static String allText(Element e)
    {
        StringBuilder sb = new StringBuilder();
        NodeList ts = e.getElementsByTagNameNS(MAIN_NS, "t");
        for (int i = 0; i < ts.getLength(); i++)
            sb.append(ts.item(i).getTextContent());
        return sb.toString();
    }

    private static String childText(Element e, String localName)
    {
        for (Node n = e.getFirstChild(); n != null; n = n.getNextSibling())
        {
            if (n instanceof Element child && localName.equals(child.getLocalName()))
                return child.getTextContent();
        }
        return null;
    }

    private static byte[] required(Map<String, byte[]> parts, String name) throws IOException
    {
        byte[] b = parts.get(name);
        if (b == null)
            throw new IOException("Not an xlsx workbook: missing " + name);
        return b;
    }

    private static Document parse(DocumentBuilder db, byte[] xml) throws Exception
    {
        try (InputStream in = new ByteArrayInputStream(xml))
        {
            return db.parse(in);
        }
    }

    private static DocumentBuilder documentBuilder() throws Exception
    {
        DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
        f.setNamespaceAware(true);
        // Workbooks are external files: no DTDs or external entities.
        f.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        f.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        f.setXIncludeAware(false);
        f.setExpandEntityReferences(false);
        return f.newDocumentBuilder();
    }
}
