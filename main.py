import os
import csv
import json
import shutil
import requests
import xml.etree.ElementTree as ET
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

visited_sitemaps = set()
visited_links = set()
dead_links = []
HEADERS = {"User-Agent": "Mozilla/5.0"}

# Thread-safe locks
visited_links_lock = Lock()
dead_links_lock = Lock()

# Common sitemap entry points
SITEMAP_PATHS = [
    "/sitemap.xml",
    "/sitemap_index.xml",
    "/sitemap-index.xml",
    "/sitemap/sitemap.xml",
    "/sitemap1.xml",
    "/sitemap/sitemap-index.xml",
]

def normalize_url(url):
    """Normalize URL for comparison and deduplication."""
    parsed = urlparse(url)
    normalized = parsed._replace(fragment='', query='', path=parsed.path.rstrip('/'))
    return normalized.geturl().lower()

def get_domain_name(url):
    parsed = urlparse(url if url.startswith("http") else "https://" + url)
    return parsed.netloc

def prepare_directory(domain):
    if os.path.exists(domain):
        shutil.rmtree(domain)
    xml_dir = os.path.join(domain, "xml")
    os.makedirs(xml_dir, exist_ok=True)
    return xml_dir

def download_sitemap(url, save_dir):
    try:
        response = requests.get(url, timeout=10, headers=HEADERS)
        if response.status_code == 200 and 'xml' in response.headers.get('Content-Type', ''):
            filename = os.path.basename(urlparse(url).path) or "sitemap.xml"
            filepath = os.path.join(save_dir, filename)
            with open(filepath, "wb") as f:
                f.write(response.content)
            print(f"✅ Downloaded sitemap: {url}")
            return filepath
    except Exception as e:
        print(f"❌ Error downloading sitemap {url}: {e}")
    return None

def parse_sitemap_for_nested_sitemaps(xml_file):
    nested_sitemaps = []
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        namespace = {'ns': root.tag.split('}')[0].strip('{')} if '}' in root.tag else {}
        for sitemap in root.findall('ns:sitemap', namespace):
            loc = sitemap.find('ns:loc', namespace)
            if loc is not None and loc.text:
                nested_sitemaps.append(loc.text.strip())
    except Exception as e:
        print(f"⚠️ Error parsing sitemap index: {xml_file} ({e})")
    return nested_sitemaps

def parse_sitemap_for_links(xml_file):
    urls = []
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        namespace = {'ns': root.tag.split('}')[0].strip('{')} if '}' in root.tag else {}
        for url in root.findall('ns:url', namespace):
            loc = url.find('ns:loc', namespace)
            if loc is not None and loc.text:
                urls.append(loc.text.strip())
    except Exception as e:
        print(f"⚠️ Error parsing sitemap links: {xml_file} ({e})")
    return urls


def check_link(url, origin, root_domain):
    try:
        response = requests.head(url, timeout=10, allow_redirects=True, headers=HEADERS)
        status = response.status_code
    except Exception as e:
        status = f"Error: {e}"

    dest = normalize_url(url)
    source = normalize_url(origin)
    dest_domain = urlparse(dest).netloc
    is_internal = "Internal" if root_domain in dest_domain else "External"

    if isinstance(status, int) and status < 400:
        return  # skip valid

    with dead_links_lock:
        dead_links.append({
            "Origin Page": source,
            "Dead Link": dest,
            "Status/Error": status,
            "Domain": dest_domain,
            "Type": is_internal
        })

def extract_links_from_page(page_url):
    found_links = []
    try:
        response = requests.get(page_url, timeout=10, headers=HEADERS)
        if "text/html" in response.headers.get("Content-Type", ""):
            soup = BeautifulSoup(response.text, 'html.parser')
            for tag in soup.find_all("a", href=True):
                href = tag["href"]
                full_url = urljoin(page_url, href)
                found_links.append(full_url)
    except Exception:
        pass
    return found_links


def process_page_and_links(link, local_file, link_file_path, executor, root_domain):
    normalized_link = normalize_url(link)
    if not is_valid_http_url(link):
        return  # skip non-http links

    with visited_links_lock:
        if normalized_link in visited_links:
            return
        visited_links.add(normalized_link)

    if normalized_link.lower().endswith((".jpg", ".png", ".pdf", ".css", ".js")):
        executor.submit(check_link, normalized_link, local_file, root_domain)
        return

    inner_links = extract_links_from_page(normalized_link)

    with open(link_file_path, "a", encoding="utf-8") as f:
        f.write(normalized_link + "\n")
        for inner in inner_links:
            if not is_valid_http_url(inner):
                continue
            norm_inner = normalize_url(inner)
            with visited_links_lock:
                if norm_inner in visited_links:
                    continue
                visited_links.add(norm_inner)

            f.write(f"  > {norm_inner}\n")
            executor.submit(check_link, norm_inner, normalized_link, root_domain)

def recursive_download(url, base_dir, executor, root_domain):
    if url in visited_sitemaps:
        return
    visited_sitemaps.add(url)

    file_base = os.path.basename(urlparse(url).path).split(".")[0] or "sitemap"
    save_dir = os.path.join(base_dir, file_base)
    os.makedirs(save_dir, exist_ok=True)

    local_file = download_sitemap(url, save_dir)
    if not local_file:
        return

    nested = parse_sitemap_for_nested_sitemaps(local_file)
    if nested:
        for nested_url in nested:
            recursive_download(nested_url, base_dir, executor, root_domain)
    else:
        page_links = parse_sitemap_for_links(local_file)
        link_file_path = os.path.join(save_dir, f"links-from-{file_base}.txt")

        futures = []
        for link in page_links:
            futures.append(executor.submit(process_page_and_links, link, local_file, link_file_path, executor, root_domain))

        for future in as_completed(futures):
            pass

def export_to_json(domain_name, dead_links):
    json_path = os.path.join(domain_name, "dead_links.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(dead_links, f, indent=2)
    print(f"🗂️ Dead links also saved to: {json_path}")

def generate_html_report(domain_name, dead_links):
    html_path = os.path.join(domain_name, "dead_links.html")

    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Dead Link Report for {domain_name}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            background: #f9f9f9;
            padding: 20px;
        }}
        h1 {{
            color: #333;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
        }}
        th, td {{
            border: 1px solid #ccc;
            padding: 8px;
            text-align: left;
        }}
        th {{
            background: #eee;
        }}
        tr.internal td {{
            background-color: #ffe6e6;
        }}
        tr.external td {{
            background-color: #e6f0ff;
        }}
        .filter {{
            margin-top: 10px;
        }}
    </style>
    <script>
        function filterTable(type) {{
            const rows = document.querySelectorAll("table tbody tr");
            rows.forEach(row => {{
                if (type === 'all') {{
                    row.style.display = '';
                }} else {{
                    row.style.display = row.classList.contains(type) ? '' : 'none';
                }}
            }});
        }}
    </script>
</head>
<body>
    <h1>Dead Link Report for {domain_name}</h1>
    <div class="filter">
        <label><input type="radio" name="filter" onclick="filterTable('all')" checked> Show All</label>
        <label><input type="radio" name="filter" onclick="filterTable('internal')"> Internal Only</label>
        <label><input type="radio" name="filter" onclick="filterTable('external')"> External Only</label>
    </div>
    <table>
        <thead>
            <tr>
                <th>Origin Page</th>
                <th>Dead Link</th>
                <th>Status/Error</th>
                <th>Domain</th>
                <th>Type</th>
            </tr>
        </thead>
        <tbody>
    """

    for link in dead_links:
        row_class = "internal" if link["Type"] == "Internal" else "external"
        html += f"""
        <tr class="{row_class}">
            <td><a href="{link['Origin Page']}" target="_blank">{link['Origin Page']}</a></td>
            <td><a href="{link['Dead Link']}" target="_blank">{link['Dead Link']}</a></td>
            <td>{link['Status/Error']}</td>
            <td>{link['Domain']}</td>
            <td>{link['Type']}</td>
        </tr>
        """

    html += """
        </tbody>
    </table>
</body>
</html>
    """

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"📊 HTML report saved to: {html_path}")

def is_valid_http_url(url):
    return url.lower().startswith("http://") or url.lower().startswith("https://")

def test_and_download_sitemaps(base_url):
    if not base_url.startswith("http"):
        base_url = "https://" + base_url

    domain_name = get_domain_name(base_url)
    xml_dir = prepare_directory(domain_name)

    with ThreadPoolExecutor(max_workers=15) as executor:
        for path in SITEMAP_PATHS:
            full_url = base_url.rstrip("/") + path
            recursive_download(full_url, xml_dir, executor, domain_name)

    with open(os.path.join(domain_name, "found_sitemaps.txt"), "w") as f:
        for url in visited_sitemaps:
            f.write(url + "\n")

    if dead_links:
        csv_path = os.path.join(domain_name, "dead_links.csv")
        with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=["Origin Page", "Dead Link", "Status/Error", "Domain", "Type"])
            writer.writeheader()
            for entry in dead_links:
                writer.writerow(entry)
        print(f"\n🚨 Dead links report saved to {csv_path}")
        export_to_json(domain_name, dead_links)
        generate_html_report(domain_name, dead_links)
    else:
        print(f"\n✅ No dead links found!")

# Entry point
if __name__ == "__main__":
    website = input("Enter website URL (e.g., example.com): ")
    test_and_download_sitemaps(website)

