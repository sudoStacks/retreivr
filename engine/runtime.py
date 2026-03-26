import os
import sys

from yt_dlp.version import __version__ as ytdlp_version
from library.provenance import get_retreivr_version


def get_runtime_info():
    return {
        "app_version": get_retreivr_version(),
        "python_version": sys.version.split()[0],
        "yt_dlp_version": ytdlp_version,
    }
