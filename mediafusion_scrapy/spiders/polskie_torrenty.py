# mediafusion_scrapy/spiders/polskie_torrenty.py

import re
from datetime import datetime
import scrapy
from mediafusion_scrapy.items import TorrentItem
from mediafusion_scrapy.spiders.common import BaseSpider

class PolskieTorrentySpider(BaseSpider):
    name = "polskie_torrenty"
    allowed_domains = ["polskie-torrenty.eu"]
    
    # --- KONFIGURACJA ---
    # Ustaw swoje ciasteczka. Te wartości pochodzą z dostarczonego pliku main.py
    cookies = {
        'pass': 'a02157fa17247ad5fa326f21d591e702',
        'uid': '178944'
    }

    # Grupy do sprawdzenia, z podziałem na filmy i seriale
    MOVIE_GROUPS = [27, 30, 71, 72, 73, 74, 75, 76, 77, 78, 79, 88, 89]
    TV_GROUPS = [25, 26, 122]
    
    # Mapowanie gatunków (opcjonalne, pipeline MediaFusion i tak analizuje tytuł)
    GENRE_MAP = {
        'Akcja': 1, 'Animowany': 2, 'Biograficzny': 3, 'Dokumentalny': 4,
        'Dramat / Obyczajowy': 5, 'Familijny': 6, 'Horror': 7, 'Katastroficzny': 8,
        'Komedia': 9, 'Komedia romantyczna': 10, 'Melodramat': 11, 'Historyczne': 12,
        'Przygodowy': 13, 'Sci-Fi / Fantasy': 14, 'Sensacyjny': 15, 'Sportowy': 16,
        'Thrillery / Kryminalne': 17, 'Wojenne / Western': 18
    }

    def start_requests(self):
        """Generuje początkowe żądania dla każdej grupy."""
        groups_to_check = self.MOVIE_GROUPS + self.TV_GROUPS
        for group_id in groups_to_check:
            m_param = 2 if group_id in self.TV_GROUPS else 1
            url = f"https://polskie-torrenty.eu/torrents.php?&category={group_id}&order=data&by=DESC&t={group_id}&m={m_param}&s=0&page=0"
            yield scrapy.Request(
                url,
                cookies=self.cookies,
                callback=self.parse,
                meta={'group_id': group_id, 'page_num': 0}
            )

    def parse(self, response):
        """Przetwarza stronę z listą torrentów."""
        group_id = response.meta['group_id']
        page_num = response.meta['page_num']

        torrent_containers = response.css('div.raised')
        if not torrent_containers:
            self.logger.info(f"Brak torrentów na stronie {page_num} dla grupy {group_id}. Kończę.")
            return

        found_new = False
        for container in torrent_containers:
            title_link = container.css('a.link2')
            if not title_link:
                continue

            raw_title = title_link.css('::text').get(default='').strip()
            href = title_link.attrib.get('href', '')
            
            magnet_match = re.search(r'id=([a-f0-9]{40})', href)
            if not magnet_match:
                continue
            
            # W MediaFusion infohash jest używany jako unikalny identyfikator
            infohash = magnet_match.group(1)

            # Sprawdzenie, czy torrent był już przetwarzany
            if self.is_duplicate_infohash(infohash):
                self.logger.debug(f"Pominięto zduplikowany torrent: {raw_title}")
                continue
            
            found_new = True
            
            # Wyodrębnienie daty dodania
            date_text_node = container.xpath("string()").re_first(r'\((\d{2}-\d{2}-\d{4})\)')
            created_at = None
            if date_text_node:
                try:
                    created_at = datetime.strptime(date_text_node, '%d-%m-%Y')
                except ValueError:
                    self.logger.warning(f"Nie udało się sparsować daty: {date_text_node}")

            # Wyodrębnienie rozmiaru
            size_text_node = container.xpath(".//b[contains(text(), 'Rozmiar:')]/following-sibling::text()[1]").get()
            size = size_text_node.strip() if size_text_node else "N/A"

            # Wyodrębnienie gatunku
            genre_text = container.css('div.link1[align="right"]::text').get(default='').strip()
            genre = None
            if '»' in genre_text:
                genre_str = genre_text.split('»')[1].strip()
                genre = self.GENRE_MAP.get(genre_str)

            # Tworzenie obiektu TorrentItem
            torrent = TorrentItem(
                infohash=infohash,
                title=raw_title,
                size=size,
                url=response.urljoin(href),
                seeders=0, # Strona nie podaje liczby seedów/peerów na liście
                leechers=0,
                created_at=created_at,
                _id=infohash,
                source=f"Polskie-Torrenty:{group_id}",
                scraped_at=datetime.now(),
                # Pola dla pipeline'ów
                parsed_data={}, 
                file_data=[],
            )
            
            # Oznaczanie typu (film/serial) dla pipeline'u
            if group_id in self.TV_GROUPS:
                torrent['type'] = 'series'
            elif group_id in self.MOVIE_GROUPS:
                torrent['type'] = 'movie'

            self.add_infohash(infohash)
            yield torrent
        
        # Logika paginacji
        if found_new:
            self.logger.info(f"Przetworzono stronę {page_num} dla grupy {group_id}. Znaleziono nowe torrenty.")
            next_page_num = page_num + 1
            m_param = 2 if group_id in self.TV_GROUPS else 1
            next_page_url = f"https://polskie-torrenty.eu/torrents.php?&category={group_id}&order=data&by=DESC&t={group_id}&m={m_param}&s=0&page={next_page_num}"
            yield scrapy.Request(
                next_page_url,
                cookies=self.cookies,
                callback=self.parse,
                meta={'group_id': group_id, 'page_num': next_page_num}
            )
        else:
            self.logger.info(f"Nie znaleziono nowych torrentów na stronie {page_num} dla grupy {group_id}. Zatrzymuję paginację.")
