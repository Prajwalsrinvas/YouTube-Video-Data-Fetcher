import concurrent.futures
import json
import os
import random
import re
import time
from datetime import datetime

import pandas as pd
import plotly.express as px
import requests
import streamlit as st
from bs4 import BeautifulSoup

st.set_page_config(
    page_title="YouTube Video Data Fetcher",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Initialize session state variables
if "urls" not in st.session_state:
    st.session_state.urls = []
if "current_df" not in st.session_state:
    st.session_state.current_df = None

# JSON cache file path
CACHE_FILE = "youtube_data_cache.json"

# Define soft blue color for visualizations
SOFT_BLUE = "#6495ED"  # Cornflower Blue
SECONDARY_COLOR = "#FF8C00"  # Dark Orange for contrast

# Maximum number of URLs that can be processed (set to None for unlimited)
MAX_URLS = 20


# Function to load cache from JSON file
def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            st.warning(f"Error loading cache: {e}. Starting with empty cache.")
    return {}


# Function to save cache to JSON file
def save_cache(cache):
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f)
    except Exception as e:
        st.warning(f"Error saving cache: {e}")


# Function to extract YouTube video ID from URL
def extract_video_id(url):
    # Regular expressions for different YouTube URL formats
    patterns = [
        r"(?:v=|\/)([0-9A-Za-z_-]{11}).*",  # Standard YouTube URLs
        r"(?:embed\/)([0-9A-Za-z_-]{11})",  # Embed URLs
        r"(?:youtu\.be\/)([0-9A-Za-z_-]{11})",  # Short URLs
    ]

    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None


# Function to extract ytInitialPlayerResponse
def extract_yt_initial_player_response(html_content):
    # Create a BeautifulSoup object
    soup = BeautifulSoup(html_content, "html.parser")

    # Find the script tag containing ytInitialPlayerResponse
    script_tags = soup.find_all("script")
    player_response = None

    # First try to find it directly in script tags
    for script in script_tags:
        if script.string and "ytInitialPlayerResponse" in script.string:
            # Use regex to extract the JSON object
            match = re.search(
                r"var ytInitialPlayerResponse\s*=\s*(\{.+?\});",
                script.string,
                re.DOTALL,
            )
            if match:
                player_response = match.group(1)
                break

    # If not found in script tags directly, try to find it in any content
    if not player_response and html_content:
        match = re.search(
            r"var ytInitialPlayerResponse\s*=\s*(\{.+?\});", html_content, re.DOTALL
        )
        if match:
            player_response = match.group(1)

    # If we found something, try to parse it as JSON
    if player_response:
        try:
            # Now try to parse it
            return json.loads(player_response)
        except json.JSONDecodeError:
            return None

    return None


# Function to fetch video data
def fetch_video_data(video_id):
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "en-US,en;q=0.9",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    }

    params = {"v": video_id}

    # Random delay between requests
    time.sleep(random.uniform(1, 2))

    try:
        response = requests.get(
            "https://www.youtube.com/watch", params=params, headers=headers
        )
        if response.status_code != 200:
            return {
                "video_id": video_id,
                "error": f"Failed to fetch: HTTP {response.status_code}",
            }

        player_response = extract_yt_initial_player_response(response.text)
        if not player_response:
            return {"video_id": video_id, "error": "Failed to extract video data"}

        # Extract relevant data
        video_details = player_response.get("videoDetails", {})
        microformat = player_response.get("microformat", {}).get(
            "playerMicroformatRenderer", {}
        )

        # Calculate duration in minutes:seconds format
        length_seconds = int(video_details.get("lengthSeconds", 0))
        duration = f"{length_seconds // 60}:{length_seconds % 60:02d}"

        # Get thumbnail URL (highest resolution)
        thumbnails = video_details.get("thumbnail", {}).get("thumbnails", [])
        thumbnail_url = thumbnails[-1]["url"] if thumbnails else ""

        # Get upload date
        upload_date = microformat.get("uploadDate", "")

        return {
            "video_id": video_id,
            "url": f"https://www.youtube.com/watch?v={video_id}",
            "title": video_details.get("title", ""),
            "duration": duration,
            "length_seconds": length_seconds,
            "keywords": ", ".join(video_details.get("keywords", [])),
            "description": video_details.get("shortDescription", ""),
            "view_count": int(video_details.get("viewCount", 0)),
            "author": video_details.get("author", ""),
            "thumbnail": thumbnail_url,
            "upload_date": upload_date,
            "category": microformat.get("category", ""),
            "is_live": video_details.get("isLiveContent", False),
            "is_family_safe": microformat.get("isFamilySafe", True),
            "error": None,
        }
    except Exception as e:
        return {"video_id": video_id, "error": str(e)}


# Function to process a list of video URLs
def process_videos(urls, progress_bar, bypass_cache=False, max_workers=5):
    video_ids = []
    valid_urls = []

    # First, validate and extract video IDs
    for url in urls:
        url = url.strip()
        if not url:
            continue

        video_id = extract_video_id(url)
        if video_id:
            video_ids.append(video_id)
            valid_urls.append(url)

    if not video_ids:
        return pd.DataFrame()

    # Load the cache from file
    cache = load_cache()

    # Identify which videos need to be fetched
    videos_to_fetch = []
    fetched_data = []

    for video_id in video_ids:
        if video_id in cache and not bypass_cache:
            fetched_data.append(cache[video_id])
        else:
            videos_to_fetch.append(video_id)

    # Fetch new videos using ThreadPoolExecutor
    if videos_to_fetch:
        total_to_fetch = len(videos_to_fetch)
        fetched_count = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_video = {
                executor.submit(fetch_video_data, video_id): video_id
                for video_id in videos_to_fetch
            }

            for future in concurrent.futures.as_completed(future_to_video):
                video_data = future.result()
                fetched_count += 1
                progress_bar.progress(
                    fetched_count / total_to_fetch,
                    text=f"Processing {fetched_count}/{total_to_fetch} videos",
                )

                # Cache the result if no error
                if not video_data.get("error"):
                    cache[video_data["video_id"]] = video_data

                fetched_data.append(video_data)

    # Save the updated cache to file
    if videos_to_fetch:
        save_cache(cache)

    # Convert to DataFrame
    df = pd.DataFrame(fetched_data)

    # Format the data
    if not df.empty and "error" in df.columns:
        # Remove rows with errors
        error_df = df[df["error"].notna()]
        df = df[df["error"].isna()]

        if not df.empty:
            # Convert view count to formatted string
            df["view_count_formatted"] = df["view_count"].apply(lambda x: f"{x:,}")

            # Format upload date
            df["upload_date_formatted"] = df["upload_date"].apply(
                lambda x: (
                    datetime.strptime(x, "%Y-%m-%dT%H:%M:%S%z").strftime("%Y-%m-%d")
                    if x
                    else ""
                )
            )

        # Add back error rows
        if not error_df.empty:
            df = pd.concat([df, error_df])

    return df


# Function to create visualizations
def create_visualizations(df):
    st.subheader("📊 Data Visualizations")

    if df.empty or "error" not in df.columns:
        st.warning("No data available for visualization")
        return

    # Clean data for visualization
    clean_df = df[df["error"].isna()].copy()

    if clean_df.empty:
        st.warning("No valid data available for visualization")
        return

    # Create tabs for different visualizations
    viz_tabs = st.tabs(
        [
            "Views Analysis",
            "Duration Analysis",
            "Uploads Timeline",
            "Channel Comparison",
        ]
    )

    with viz_tabs[0]:
        st.subheader("Views Distribution")

        col1, col2 = st.columns(2)

        with col1:
            # View count distribution
            fig = px.histogram(
                clean_df,
                x="view_count",
                nbins=20,
                title="Distribution of View Counts",
                labels={"view_count": "View Count", "count": "Number of Videos"},
                color_discrete_sequence=[SOFT_BLUE],  # Soft blue
            )
            fig.update_layout(bargap=0.1)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Top videos by views
            top_videos = clean_df.nlargest(10, "view_count")
            fig = px.bar(
                top_videos,
                y="title",
                x="view_count",
                orientation="h",
                title="Top 10 Videos by View Count",
                labels={"title": "Video Title", "view_count": "Views"},
                color_discrete_sequence=[SOFT_BLUE],  # Soft blue
            )
            fig.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig, use_container_width=True)

    with viz_tabs[1]:
        st.subheader("Video Duration Analysis")

        col1, col2 = st.columns(2)

        with col1:
            # Duration distribution
            fig = px.histogram(
                clean_df,
                x="length_seconds",
                nbins=20,
                title="Distribution of Video Durations",
                labels={
                    "length_seconds": "Duration (seconds)",
                    "count": "Number of Videos",
                },
                color_discrete_sequence=[SOFT_BLUE],  # Soft blue
            )
            fig.update_layout(bargap=0.1)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Relationship between duration and views
            fig = px.scatter(
                clean_df,
                x="length_seconds",
                y="view_count",
                hover_name="title",
                size="view_count",
                size_max=50,
                title="Relationship Between Duration and Views",
                labels={
                    "length_seconds": "Duration (seconds)",
                    "view_count": "Views",
                    "author": "Channel",
                },
                color="author",
            )
            st.plotly_chart(fig, use_container_width=True)

    with viz_tabs[2]:
        st.subheader("Upload Timeline")

        # Fix for datetime conversion
        has_timeline_data = False

        if "upload_date" in clean_df.columns:
            try:
                # Format the date string to datetime
                clean_df["upload_date_iso"] = clean_df["upload_date"].apply(
                    lambda x: x.split("T")[0] if pd.notna(x) and "T" in x else None
                )

                # Convert to datetime
                clean_df["upload_datetime"] = pd.to_datetime(
                    clean_df["upload_date_iso"], errors="coerce"
                )

                # Only proceed if we have valid datetime values
                if clean_df["upload_datetime"].notna().any():
                    clean_df["upload_year"] = clean_df["upload_datetime"].dt.year
                    clean_df["upload_month"] = clean_df["upload_datetime"].dt.month
                    clean_df["upload_yearmonth"] = clean_df[
                        "upload_datetime"
                    ].dt.strftime("%Y-%m")
                    has_timeline_data = True
            except Exception as e:
                st.warning(f"Error processing upload dates: {str(e)}")

        if has_timeline_data:
            # Group by upload month
            timeline_data = (
                clean_df.groupby("upload_yearmonth").size().reset_index(name="count")
            )
            timeline_data = timeline_data.sort_values("upload_yearmonth")

            if not timeline_data.empty:
                fig = px.line(
                    timeline_data,
                    x="upload_yearmonth",
                    y="count",
                    title="Videos Uploaded by Month",
                    labels={"upload_yearmonth": "Month", "count": "Number of Videos"},
                    markers=True,
                    color_discrete_sequence=[SOFT_BLUE],  # Soft blue
                )
                st.plotly_chart(fig, use_container_width=True)

                # View count over time
                view_time_data = (
                    clean_df.groupby("upload_yearmonth")["view_count"]
                    .mean()
                    .reset_index()
                )
                view_time_data = view_time_data.sort_values("upload_yearmonth")

                if not view_time_data.empty:
                    fig = px.line(
                        view_time_data,
                        x="upload_yearmonth",
                        y="view_count",
                        title="Average Views by Upload Month",
                        labels={
                            "upload_yearmonth": "Month",
                            "view_count": "Average Views",
                        },
                        markers=True,
                        color_discrete_sequence=[SECONDARY_COLOR],  # Secondary color
                    )
                    st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning(
                "Upload date information not available for timeline visualization"
            )

    with viz_tabs[3]:
        st.subheader("Channel Comparison")

        # Videos per channel
        channel_counts = clean_df["author"].value_counts().reset_index()
        channel_counts.columns = ["author", "count"]

        fig = px.bar(
            channel_counts,
            y="author",
            x="count",
            orientation="h",
            title="Number of Videos per Channel",
            labels={"author": "Channel", "count": "Number of Videos"},
            color_discrete_sequence=[SOFT_BLUE],  # Soft blue
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)

        # Average views per channel
        channel_views = clean_df.groupby("author")["view_count"].mean().reset_index()
        channel_views = channel_views.sort_values("view_count", ascending=False)

        fig = px.bar(
            channel_views,
            y="author",
            x="view_count",
            orientation="h",
            title="Average Views per Channel",
            labels={"author": "Channel", "view_count": "Average Views"},
            color_discrete_sequence=[SECONDARY_COLOR],  # Secondary color
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)


# Function to filter dataframe
def filter_dataframe(df, author_filter, category_filter, date_filter, search_query):
    filtered_df = df.copy()

    # Apply filters
    if author_filter and "All" not in author_filter:
        filtered_df = filtered_df[filtered_df["author"].isin(author_filter)]

    if category_filter and "All" not in category_filter:
        filtered_df = filtered_df[filtered_df["category"].isin(category_filter)]

    if date_filter and date_filter != "All":
        if date_filter == "Today":
            today = datetime.now().strftime("%Y-%m-%d")
            filtered_df = filtered_df[filtered_df["upload_date_formatted"] == today]
        elif date_filter == "This Week":
            today = datetime.now()
            week_ago = (today - pd.Timedelta(days=7)).strftime("%Y-%m-%d")
            today = today.strftime("%Y-%m-%d")
            mask = (filtered_df["upload_date_formatted"] >= week_ago) & (
                filtered_df["upload_date_formatted"] <= today
            )
            filtered_df = filtered_df[mask]
        elif date_filter == "This Month":
            current_month = datetime.now().strftime("%Y-%m")
            filtered_df = filtered_df[
                filtered_df["upload_date_formatted"].str.startswith(
                    current_month, na=False
                )
            ]
        elif date_filter == "This Year":
            current_year = datetime.now().strftime("%Y")
            filtered_df = filtered_df[
                filtered_df["upload_date_formatted"].str.startswith(
                    current_year, na=False
                )
            ]

    # Apply search query
    if search_query:
        search_query = search_query.lower()
        filtered_df = filtered_df[
            filtered_df["title"].str.lower().str.contains(search_query, na=False)
            | filtered_df["description"]
            .str.lower()
            .str.contains(search_query, na=False)
            | filtered_df["keywords"].str.lower().str.contains(search_query, na=False)
        ]

    return filtered_df


# Function to display results
def display_results(df):
    # Handle errors
    error_df = (
        df[df["error"].notna()].copy() if "error" in df.columns else pd.DataFrame()
    )
    success_df = df[df["error"].isna()].copy() if "error" in df.columns else df.copy()

    # Display error summary if there are errors
    if not error_df.empty:
        st.warning(f"Failed to process {len(error_df)} out of {len(df)} videos")
        with st.expander("View Errors"):
            st.dataframe(error_df[["video_id", "error"]], use_container_width=True)

    if success_df.empty:
        st.error("No videos were successfully processed.")
        return

    # Get filters from sidebar
    st.sidebar.header("Filter Videos")

    # Search
    search_query = st.sidebar.text_input("Search in title, description, keywords")

    # Author filter
    authors = ["All"] + sorted(success_df["author"].unique().tolist())
    author_filter = st.sidebar.multiselect("Filter by Channel", authors)

    # Category filter
    categories = ["All"] + sorted(
        [cat for cat in success_df["category"].unique() if cat]
    )
    category_filter = st.sidebar.multiselect("Filter by Category", categories)

    # Date filter
    date_options = ["All", "Today", "This Week", "This Month", "This Year"]
    date_filter = st.sidebar.selectbox("Filter by Upload Date", date_options)

    # Apply filters
    filtered_df = filter_dataframe(
        success_df, author_filter, category_filter, date_filter, search_query
    )

    # Display results count
    st.subheader(f"`Results: {len(filtered_df)} Videos`")
    if len(filtered_df) < len(success_df):
        st.caption(
            f"Showing {len(filtered_df)} of {len(success_df)} videos (filtered view)"
        )

    # Prepare for display
    display_df = filtered_df.copy()

    # Format display data
    if "view_count_formatted" not in display_df.columns:
        display_df["view_count_formatted"] = display_df["view_count"].apply(
            lambda x: f"{x:,}" if pd.notna(x) else ""
        )

    if (
        "upload_date_formatted" not in display_df.columns
        and "upload_date" in display_df.columns
    ):
        display_df["upload_date_formatted"] = display_df["upload_date"].apply(
            lambda x: (
                datetime.strptime(x, "%Y-%m-%dT%H:%M:%S%z").strftime("%Y-%m-%d")
                if pd.notna(x) and x
                else ""
            )
        )

    # Create truncated description
    display_df["short_description"] = display_df["description"].apply(
        lambda x: (x[:100] + "...") if pd.notna(x) and len(x) > 100 else x
    )

    # Rename columns for display
    display_df = display_df.rename(
        columns={
            "view_count_formatted": "Views",
            "upload_date_formatted": "Upload Date",
            "duration": "Duration",
            "title": "Title",
            "author": "Channel",
            "category": "Category",
            "thumbnail": "Thumbnail",  # Using actual thumbnail URL for the ImageColumn
            "short_description": "Description",
            "keywords": "Keywords",
        }
    )

    # Columns to display (with thumbnail first)
    display_cols = [
        "Thumbnail",
        "Title",
        "Channel",
        "Duration",
        "Views",
        "Upload Date",
        "Category",
        "Description",
        "Keywords",
    ]

    # Display the data table with thumbnails
    st.dataframe(
        display_df[display_cols],
        use_container_width=True,
        column_config={
            "Thumbnail": st.column_config.ImageColumn(
                "Thumbnail", help="Video thumbnail"
            ),
            "Title": st.column_config.TextColumn(
                "Title",
                width="large",
            ),
            "Description": st.column_config.TextColumn(
                "Description",
                width="medium",
            ),
            "Keywords": st.column_config.TextColumn(
                "Keywords",
                width="medium",
            ),
        },
        hide_index=True,
    )

    # Create visualizations for the filtered data
    create_visualizations(filtered_df)

    # Export options
    st.subheader("Export/Copy Options")

    col1, col2 = st.columns(2)

    with col1:
        # Export to CSV
        if st.button("Export to CSV", use_container_width=True):
            csv = filtered_df.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"youtube_videos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                use_container_width=True,
            )

    with col2:
        # Copy all displayed URLs
        if st.button("Copy URLs", use_container_width=True):
            urls_to_copy = filtered_df["url"].tolist()
            st.code("\n".join(urls_to_copy))
            st.success(f"✓ {len(urls_to_copy)} URLs ready to copy")


# Main app function
def main():
    st.header("YouTube Video Data Fetcher")

    # Display URL limit info if applicable
    if MAX_URLS is not None:
        st.info(
            f"ℹ️ This app is limited to processing a maximum of {MAX_URLS} URLs at once."
        )

    # Tabs for input methods
    with st.sidebar:
        tab1, tab2 = st.tabs(["Paste URLs", "Upload File"])

        with tab1:
            url_input = st.text_area(
                label="Enter YouTube URLs",
                placeholder="https://www.youtube.com/watch?v=...",
                help="Enter one URL per line",
            )
            if url_input:
                st.session_state.urls = url_input.split("\n")

        with tab2:
            uploaded_file = st.file_uploader("Upload a file with URLs", type=["txt"])
            if uploaded_file is not None:
                st.session_state.urls = (
                    uploaded_file.getvalue().decode("utf-8").split("\n")
                )
                st.success(f"Loaded {len(st.session_state.urls)} URLs from file")

        # Option to bypass cache
        bypass_cache = st.toggle(
            "Bypass cache", help="Force refresh all data even if cached", value=False
        )

        max_workers = st.slider(
            "Max Concurrent Requests",
            1,
            500,
            50,
            help="Higher values process faster but might trigger YouTube's rate limits",
        )

        process_button = st.button("Process Videos", use_container_width=True)

    # Clean up URLs (remove empty lines)
    clean_urls = [url.strip() for url in st.session_state.urls if url.strip()]

    # Apply the URL limit if needed
    if MAX_URLS is not None and len(clean_urls) > MAX_URLS:
        excess_count = len(clean_urls) - MAX_URLS
        clean_urls = clean_urls[:MAX_URLS]
        st.warning(
            f"⚠️ Only processing the first {MAX_URLS} URLs. {excess_count} additional URLs were ignored due to the limit."
        )

    url_count = len(clean_urls)
    if url_count > 0:
        st.toast(f"{url_count} URLs ready to process")

    # Display cache info
    cache = load_cache()
    if cache:
        st.sidebar.caption(f"Cache contains {len(cache)} videos")

    # Process the URLs if the button is clicked
    if process_button and clean_urls:
        with st.spinner("Processing videos..."):
            progress_bar = st.progress(0, text="Preparing to process videos...")
            df = process_videos(clean_urls, progress_bar, bypass_cache, max_workers)
            progress_bar.empty()

        if not df.empty:
            st.session_state["current_df"] = df
            display_results(df)
        else:
            st.error("No valid YouTube URLs found.")
    elif process_button:
        st.warning("Please enter at least one YouTube URL.")

    # Display previous results if available
    elif (
        "current_df" in st.session_state and st.session_state["current_df"] is not None
    ):
        display_results(st.session_state["current_df"])


# Run the app
if __name__ == "__main__":
    main()
