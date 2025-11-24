import requests
import pandas as pd
import time
import os

TMDB_API_KEY = "YOUR_API_KEY_HERE"
OUTPUT_FILE = "tmdb_movies_full.csv"

YEARS = range(2000, 2025)     # Lấy từ năm 2000 → 2024
MAX_PAGES = 20                # Mỗi năm tối đa ~400 phim (20 trang * 20 phim)

# ========================================================
# 1. Hàm gọi API TMDB an toàn (tự retry)
# ========================================================
def tmdb_get(url, params=None, max_retry=3):
    if params is None: params = {}
    params["api_key"] = TMDB_API_KEY
    
    for _ in range(max_retry):
        try:
            r = requests.get(url, params=params, timeout=10)
            if r.status_code == 200:
                return r.json()
            time.sleep(1)
        except:
            time.sleep(1)
    return None

# ========================================================
# 2. Lấy danh sách phim của 1 năm bằng discover
# ========================================================
def discover_movies_by_year(year):
    movies = []
    
    for page in range(1, MAX_PAGES + 1):
        print(f"  → Discover year {year}, page {page}")

        data = tmdb_get(
            "https://api.themoviedb.org/3/discover/movie",
            {
                "primary_release_year": year,
                "sort_by": "revenue.desc",
                "page": page
            }
        )

        if not data or "results" not in data:
            break

        movies.extend(data["results"])

        # Dừng nếu hết phim
        if page >= data.get("total_pages", 1):
            break

        time.sleep(0.3)

    return movies

# ========================================================
# 3. Lấy chi tiết 1 phim (budget, revenue, runtime, genres…)
# ========================================================
def get_movie_details(movie_id):
    data = tmdb_get(
        f"https://api.themoviedb.org/3/movie/{movie_id}",
        {"append_to_response": "credits"}
    )
    if not data:
        return None

    # Lấy đạo diễn
    director = "Unknown"
    for member in data.get("credits", {}).get("crew", []):
        if member["job"] == "Director":
            director = member["name"]
            break

    # 3 diễn viên chính
    cast = data.get("credits", {}).get("cast", [])
    top_cast = ", ".join(c["name"] for c in cast[:3])

    # Genres
    genres = ", ".join(g["name"] for g in data.get("genres", []))

    # Production companies
    companies = ", ".join(c["name"] for c in data.get("production_companies", []))

    return {
        "tmdb_id": movie_id,
        "title": data.get("title"),
        "release_date": data.get("release_date"),
        "budget": data.get("budget"),
        "revenue": data.get("revenue"),          # TARGET prediction
        "runtime": data.get("runtime"),
        "vote_average": data.get("vote_average"),
        "vote_count": data.get("vote_count"),
        "genres": genres,
        "production_companies": companies,
        "director": director,
        "top_cast": top_cast
    }

# ========================================================
# 4. MAIN — Lấy toàn bộ dataset
# ========================================================
def main():
    all_rows = []
    total = 0

    print("\n======= BẮT ĐẦU THU THẬP DỮ LIỆU TMDB =======\n")

    for year in YEARS:
        print(f"\n----------------------------------------")
        print(f"📌 Năm {year}")
        print("----------------------------------------")

        discover_list = discover_movies_by_year(year)
        print(f"  → Tìm thấy {len(discover_list)} phim")

        for mv in discover_list:
            movie_id = mv["id"]
            print(f"    - Lấy details ID {movie_id}...")

            details = get_movie_details(movie_id)
            if details:
                all_rows.append(details)
                total += 1

            time.sleep(0.3)  # Bảo vệ API

        # Lưu checkpoint mỗi năm
        df = pd.DataFrame(all_rows)
        df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
        print(f"  → Đã lưu checkpoint ({len(all_rows)} phim tổng cộng)")

    print("\n======= HOÀN TẤT =======")
    print(f"📁 File lưu tại: {OUTPUT_FILE}")
    print(f"📊 Tổng số phim: {total}")

if __name__ == "__main__":
    main()
